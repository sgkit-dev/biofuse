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
        buf = bed_protocol.pack_open_request("small.bed")
        assert buf[:1] == b"O"
        (name_len,) = struct.unpack("<I", buf[1:5])
        assert name_len == len("small.bed")
        assert buf[5:].decode("utf-8") == "small.bed"

    def test_open_request_utf8_multibyte(self):
        name = "tëst.bed"
        buf = bed_protocol.pack_open_request(name)
        (name_len,) = struct.unpack("<I", buf[1:5])
        assert name_len == len(name.encode("utf-8"))
        assert buf[5:].decode("utf-8") == name

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
    def test_open_length_header_and_payload_roundtrip(self):
        name = "alt.fam"
        buf = bed_protocol.pack_open_request(name)
        # Skip the tag byte; transport reads it separately.
        body = buf[1:]
        name_len = bed_protocol.parse_open_length_header(body[:4])
        payload = body[4 : 4 + name_len]
        assert bed_protocol.parse_open_payload(payload) == name

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
            bed_protocol.FileSpec("a.bed", 1024, 0o100444),
            bed_protocol.FileSpec("a.bim", 100, 0o100444),
            bed_protocol.FileSpec("a.fam", 50, 0o100444),
        ]
        buf = bed_protocol.pack_list_reply(entries)
        status = bed_protocol.parse_status(buf[:8])
        assert status == 3
        offset = 8
        seen = []
        for _ in range(status):
            hdr = buf[offset : offset + bed_protocol.REPLY_LIST_ENTRY_HDR_SIZE]
            name_len, size, mode = bed_protocol.parse_list_entry_header(hdr)
            offset += bed_protocol.REPLY_LIST_ENTRY_HDR_SIZE
            name = buf[offset : offset + name_len].decode("utf-8")
            offset += name_len
            seen.append(bed_protocol.FileSpec(name, size, mode))
        assert seen == entries
        assert offset == len(buf)

    def test_open_reply_roundtrip(self):
        buf = bed_protocol.pack_open_reply(handle=11, size=2048, mode=0o100444)
        status = bed_protocol.parse_status(buf[:8])
        assert status == 0
        body = buf[8 : 8 + bed_protocol.REPLY_OPEN_BODY_SIZE]
        assert bed_protocol.parse_open_body(body) == (11, 2048, 0o100444)

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

    def test_filenotfound_carries_enoent(self):
        # FileNotFoundError sets errno=ENOENT automatically.
        exc = FileNotFoundError(errno.ENOENT, "missing")
        assert bed_protocol.errno_for_exception(exc) == errno.ENOENT

    def test_arbitrary_exception_falls_back_to_eio(self):
        assert bed_protocol.errno_for_exception(RuntimeError("boom")) == errno.EIO
