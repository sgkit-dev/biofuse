"""Pure unit tests for the bed-worker wire protocol.

The protocol module has no I/O — these tests pin its byte layouts and
roundtrip semantics.
"""

import errno
import struct

import pytest

from biofuse import bed_protocol


class TestRequestFraming:
    def test_list_request_is_seq_then_tag(self):
        buf = bed_protocol.pack_list_request(123)
        seq = bed_protocol.parse_seq(buf[:8])
        assert seq == 123
        assert buf[8:9] == b"L"
        assert len(buf) == bed_protocol.REQ_HEADER_SIZE

    def test_open_request_layout(self):
        buf = bed_protocol.pack_open_request(7, 42, bed_protocol.FileType.BED)
        assert bed_protocol.parse_seq(buf[:8]) == 7
        assert buf[8:9] == b"O"
        fh, file_type = struct.unpack("<QB", buf[9:])
        assert (fh, file_type) == (42, int(bed_protocol.FileType.BED))

    def test_read_request_layout(self):
        buf = bed_protocol.pack_read_request(99, 7, 4096, 8192)
        assert bed_protocol.parse_seq(buf[:8]) == 99
        assert buf[8:9] == b"R"
        h, off, sz = struct.unpack("<QQQ", buf[9:])
        assert (h, off, sz) == (7, 4096, 8192)

    def test_close_request_layout(self):
        buf = bed_protocol.pack_close_request(5, 42)
        assert bed_protocol.parse_seq(buf[:8]) == 5
        assert buf[8:9] == b"C"
        (h,) = struct.unpack("<Q", buf[9:])
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
        buf = bed_protocol.pack_open_request(0, 123, file_type)
        assert bed_protocol.parse_open_payload(buf[9:]) == (123, file_type)

    def test_read_payload_roundtrip(self):
        buf = bed_protocol.pack_read_request(0, 123, 1, 2)
        assert bed_protocol.parse_read_payload(buf[9:]) == (123, 1, 2)

    def test_close_payload_roundtrip(self):
        buf = bed_protocol.pack_close_request(0, 0xDEADBEEF)
        assert bed_protocol.parse_close_payload(buf[9:]) == 0xDEADBEEF


class TestReplyFraming:
    def test_error_reply_negates_errno(self):
        buf = bed_protocol.pack_error_reply(11, errno.ENOENT)
        seq, status = bed_protocol.parse_reply_header(buf)
        assert seq == 11
        assert status == -errno.ENOENT

    def test_error_reply_rejects_non_positive_errno(self):
        with pytest.raises(ValueError, match="positive errno"):
            bed_protocol.pack_error_reply(0, 0)
        with pytest.raises(ValueError, match="positive errno"):
            bed_protocol.pack_error_reply(0, -1)

    def test_list_reply_zero_entries(self):
        buf = bed_protocol.pack_list_reply(7, [])
        seq, status = bed_protocol.parse_reply_header(buf)
        assert seq == 7
        assert status == 0
        assert len(buf) == bed_protocol.REPLY_HEADER_SIZE

    def test_list_reply_three_entries_roundtrip(self):
        entries = [
            (bed_protocol.FileType.BED, 1024),
            (bed_protocol.FileType.BIM, 100),
            (bed_protocol.FileType.FAM, 50),
        ]
        buf = bed_protocol.pack_list_reply(99, entries)
        seq, status = bed_protocol.parse_reply_header(
            buf[: bed_protocol.REPLY_HEADER_SIZE]
        )
        assert seq == 99
        # status is the body byte length (3 entries × entry size).
        assert status == 3 * bed_protocol.REPLY_LIST_ENTRY_SIZE
        offset = bed_protocol.REPLY_HEADER_SIZE
        seen = []
        while offset < len(buf):
            entry = buf[offset : offset + bed_protocol.REPLY_LIST_ENTRY_SIZE]
            offset += bed_protocol.REPLY_LIST_ENTRY_SIZE
            seen.append(bed_protocol.parse_list_entry(entry))
        assert seen == entries
        assert offset == len(buf)

    def test_open_reply_is_zero_status(self):
        buf = bed_protocol.pack_open_reply(3)
        seq, status = bed_protocol.parse_reply_header(buf)
        assert (seq, status) == (3, 0)
        assert len(buf) == bed_protocol.REPLY_HEADER_SIZE

    def test_read_reply_roundtrip(self):
        data = b"hello, world\x00\x01\x02"
        buf = bed_protocol.pack_read_reply(17, data)
        seq, status = bed_protocol.parse_reply_header(
            buf[: bed_protocol.REPLY_HEADER_SIZE]
        )
        assert seq == 17
        assert status == len(data)
        assert buf[bed_protocol.REPLY_HEADER_SIZE :] == data

    def test_read_reply_empty_payload(self):
        buf = bed_protocol.pack_read_reply(2, b"")
        seq, status = bed_protocol.parse_reply_header(buf)
        assert (seq, status) == (2, 0)
        assert len(buf) == bed_protocol.REPLY_HEADER_SIZE

    def test_close_reply_is_zero_status(self):
        buf = bed_protocol.pack_close_reply(8)
        seq, status = bed_protocol.parse_reply_header(buf)
        assert (seq, status) == (8, 0)
        assert len(buf) == bed_protocol.REPLY_HEADER_SIZE


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
