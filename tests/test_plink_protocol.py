"""Pure unit tests for the plink-server wire protocol.

The protocol module has no I/O — these tests pin its byte layouts and
roundtrip semantics.
"""

import errno
import struct

import pytest

from biofuse import plink_protocol


class TestRequestFraming:
    def test_get_metadata_request_is_just_tag(self):
        assert plink_protocol.pack_get_metadata_request() == b"M"

    def test_read_request_layout(self):
        buf = plink_protocol.pack_read_request(4096, 8192)
        assert buf[:1] == b"R"
        off, size = struct.unpack("<QQ", buf[1:])
        assert (off, size) == (4096, 8192)
        assert len(buf) == 1 + plink_protocol.REQ_READ_PAYLOAD_SIZE


class TestRequestParsing:
    def test_read_payload_roundtrip(self):
        buf = plink_protocol.pack_read_request(0xDEADBEEF, 0xCAFE)
        off, size = plink_protocol.parse_read_payload(buf[1:])
        assert (off, size) == (0xDEADBEEF, 0xCAFE)


class TestReplyFraming:
    def test_error_reply_negates_errno(self):
        buf = plink_protocol.pack_error_reply(errno.ENOENT)
        assert plink_protocol.parse_status(buf) == -errno.ENOENT
        assert len(buf) == plink_protocol.REPLY_STATUS_SIZE

    def test_error_reply_rejects_non_positive_errno(self):
        with pytest.raises(ValueError, match="positive errno"):
            plink_protocol.pack_error_reply(0)
        with pytest.raises(ValueError, match="positive errno"):
            plink_protocol.pack_error_reply(-1)

    def test_metadata_reply_layout(self):
        bim = b"chr1\trs1\t0\t100\tA\tT\n"
        fam = b"FID IID 0 0 0 -9\n"
        bed_size = 1024
        buf = plink_protocol.pack_metadata_reply(bim, fam, bed_size)
        status = plink_protocol.parse_status(buf[: plink_protocol.REPLY_STATUS_SIZE])
        # status = body length = header + bim + fam
        expected_body = plink_protocol.META_HEADER_SIZE + len(bim) + len(fam)
        assert status == expected_body
        body_start = plink_protocol.REPLY_STATUS_SIZE
        header_end = body_start + plink_protocol.META_HEADER_SIZE
        bim_size, fam_size, got_bed_size = plink_protocol.parse_metadata_header(
            buf[body_start:header_end]
        )
        assert (bim_size, fam_size, got_bed_size) == (len(bim), len(fam), bed_size)
        bim_end = header_end + bim_size
        fam_end = bim_end + fam_size
        assert buf[header_end:bim_end] == bim
        assert buf[bim_end:fam_end] == fam
        assert len(buf) == fam_end

    def test_metadata_reply_empty_static_files(self):
        buf = plink_protocol.pack_metadata_reply(b"", b"", 0)
        status = plink_protocol.parse_status(buf[: plink_protocol.REPLY_STATUS_SIZE])
        assert status == plink_protocol.META_HEADER_SIZE

    def test_read_reply_roundtrip(self):
        data = b"hello, world\x00\x01\x02"
        buf = plink_protocol.pack_read_reply(data)
        status = plink_protocol.parse_status(buf[: plink_protocol.REPLY_STATUS_SIZE])
        assert status == len(data)
        assert buf[plink_protocol.REPLY_STATUS_SIZE :] == data

    def test_read_reply_empty(self):
        buf = plink_protocol.pack_read_reply(b"")
        assert plink_protocol.parse_status(buf) == 0
        assert len(buf) == plink_protocol.REPLY_STATUS_SIZE


class TestStatusToError:
    def test_negative_status_becomes_oserror(self):
        err = plink_protocol.status_to_error(-errno.EBADF)
        assert isinstance(err, OSError)
        assert err.errno == errno.EBADF

    def test_non_negative_status_rejected(self):
        with pytest.raises(ValueError, match="negative"):
            plink_protocol.status_to_error(0)
        with pytest.raises(ValueError, match="negative"):
            plink_protocol.status_to_error(7)


class TestErrnoForException:
    def test_oserror_errno_propagates(self):
        exc = OSError(errno.ENOENT, "missing")
        assert plink_protocol.errno_for_exception(exc) == errno.ENOENT

    def test_arbitrary_exception_falls_back_to_eio(self):
        assert plink_protocol.errno_for_exception(RuntimeError("boom")) == errno.EIO
