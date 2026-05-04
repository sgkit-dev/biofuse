"""Pure unit tests for the bed-worker wire protocol.

The protocol module has no I/O — these tests pin its byte layouts and
roundtrip semantics.
"""

import errno
import struct

import pytest

from biofuse import bed_protocol


class TestRequestFraming:
    def test_list_request_is_just_tag(self):
        assert bed_protocol.pack_list_request() == b"L"

    def test_open_request_layout(self):
        buf = bed_protocol.pack_open_request(7, bed_protocol.FileType.BED)
        assert buf[:1] == b"O"
        fh, file_type = struct.unpack("<QB", buf[1:])
        assert (fh, file_type) == (7, int(bed_protocol.FileType.BED))

    def test_read_request_layout(self):
        buf = bed_protocol.pack_read_request(7, 4096, 8192)
        assert buf[:1] == b"R"
        h, off, sz = struct.unpack("<QQQ", buf[1:])
        assert (h, off, sz) == (7, 4096, 8192)

    def test_close_request_layout(self):
        buf = bed_protocol.pack_close_request(42)
        assert buf[:1] == b"C"
        (h,) = struct.unpack("<Q", buf[1:])
        assert h == 42


class TestRequestParsing:
    @pytest.mark.parametrize(
        "file_type",
        [
            bed_protocol.FileType.BED,
            bed_protocol.FileType.BIM,
            bed_protocol.FileType.FAM,
        ],
    )
    def test_open_payload_roundtrip(self, file_type):
        buf = bed_protocol.pack_open_request(123, file_type)
        assert bed_protocol.parse_open_payload(buf[1:]) == (123, file_type)

    def test_read_payload_roundtrip(self):
        buf = bed_protocol.pack_read_request(123, 1, 2)
        assert bed_protocol.parse_read_payload(buf[1:]) == (123, 1, 2)

    def test_close_payload_roundtrip(self):
        buf = bed_protocol.pack_close_request(0xDEADBEEF)
        assert bed_protocol.parse_close_payload(buf[1:]) == 0xDEADBEEF


class TestReplyFraming:
    def test_error_reply_negates_errno(self):
        buf = bed_protocol.pack_error_reply(errno.ENOENT)
        (status,) = struct.unpack("<q", buf)
        assert status == -errno.ENOENT

    def test_error_reply_rejects_non_positive_errno(self):
        with pytest.raises(ValueError, match="positive errno"):
            bed_protocol.pack_error_reply(0)
        with pytest.raises(ValueError, match="positive errno"):
            bed_protocol.pack_error_reply(-1)

    def test_list_reply_zero_entries(self):
        buf = bed_protocol.pack_list_reply([])
        assert bed_protocol.parse_status(buf[:8]) == 0
        assert len(buf) == 8

    def test_list_reply_three_entries_roundtrip(self):
        entries = [
            (bed_protocol.FileType.BED, 1024),
            (bed_protocol.FileType.BIM, 100),
            (bed_protocol.FileType.FAM, 50),
        ]
        buf = bed_protocol.pack_list_reply(entries)
        status = bed_protocol.parse_status(buf[:8])
        assert status == 3
        offset = 8
        seen = []
        for _ in range(status):
            entry = buf[offset : offset + bed_protocol.REPLY_LIST_ENTRY_SIZE]
            offset += bed_protocol.REPLY_LIST_ENTRY_SIZE
            seen.append(bed_protocol.parse_list_entry(entry))
        assert seen == entries
        assert offset == len(buf)

    def test_open_reply_is_zero_status(self):
        buf = bed_protocol.pack_open_reply()
        assert bed_protocol.parse_status(buf) == 0
        assert len(buf) == bed_protocol.REPLY_STATUS_SIZE

    def test_read_reply_roundtrip(self):
        data = b"hello, world\x00\x01\x02"
        buf = bed_protocol.pack_read_reply(data)
        status = bed_protocol.parse_status(buf[:8])
        assert status == len(data)
        assert buf[8:] == data

    def test_read_reply_empty_payload(self):
        buf = bed_protocol.pack_read_reply(b"")
        assert bed_protocol.parse_status(buf[:8]) == 0
        assert len(buf) == 8

    def test_close_reply_is_zero_status(self):
        buf = bed_protocol.pack_close_reply()
        assert bed_protocol.parse_status(buf) == 0
        assert len(buf) == 8


class TestStatusToError:
    def test_negative_status_becomes_oserror(self):
        err = bed_protocol.status_to_error(-errno.EBADF)
        assert isinstance(err, OSError)
        assert err.errno == errno.EBADF

    def test_non_negative_status_rejected(self):
        with pytest.raises(ValueError, match="negative"):
            bed_protocol.status_to_error(0)
        with pytest.raises(ValueError, match="negative"):
            bed_protocol.status_to_error(7)


class TestErrnoForException:
    def test_oserror_errno_propagates(self):
        exc = OSError(errno.ENOENT, "missing")
        assert bed_protocol.errno_for_exception(exc) == errno.ENOENT

    def test_arbitrary_exception_falls_back_to_eio(self):
        assert bed_protocol.errno_for_exception(RuntimeError("boom")) == errno.EIO
