"""Pure unit tests for the encoder-server wire protocol.

The protocol module has no I/O — these tests pin its byte layouts and
roundtrip semantics. Metadata-reply shape is variable-arity (any
number of static-file bodies in any order, plus one streaming-file
size); both 2-static layouts used in production (PLINK bim/fam, BGEN
sample/bgi) are exercised here.
"""

import errno
import struct

import pytest

from biofuse import encoder_protocol


class TestRequestFraming:
    def test_get_metadata_request_is_just_tag(self):
        assert encoder_protocol.pack_get_metadata_request() == b"M"

    def test_read_request_layout(self):
        buf = encoder_protocol.pack_read_request(4096, 8192)
        assert buf[:1] == b"R"
        off, size = struct.unpack("<QQ", buf[1:])
        assert (off, size) == (4096, 8192)
        assert len(buf) == 1 + encoder_protocol.REQ_READ_PAYLOAD_SIZE


class TestRequestParsing:
    def test_read_payload_roundtrip(self):
        buf = encoder_protocol.pack_read_request(0xDEADBEEF, 0xCAFE)
        off, size = encoder_protocol.parse_read_payload(buf[1:])
        assert (off, size) == (0xDEADBEEF, 0xCAFE)


class TestReplyFraming:
    def test_error_reply_negates_errno(self):
        buf = encoder_protocol.pack_error_reply(errno.ENOENT)
        assert encoder_protocol.parse_status(buf) == -errno.ENOENT
        assert len(buf) == encoder_protocol.REPLY_STATUS_SIZE

    def test_error_reply_rejects_non_positive_errno(self):
        with pytest.raises(ValueError, match="positive errno"):
            encoder_protocol.pack_error_reply(0)
        with pytest.raises(ValueError, match="positive errno"):
            encoder_protocol.pack_error_reply(-1)

    @pytest.mark.parametrize(
        ("static_bodies", "stream_size"),
        [
            (
                [b"chr1\trs1\t0\t100\tA\tT\n", b"FID IID 0 0 0 -9\n"],
                1024,
            ),
            (
                [b"ID_1 ID_2 missing\n0 0 0\nA A 0\n", b"SQLite-format-3\x00" * 10],
                65536,
            ),
            ([b"", b""], 0),
            ([b"only-one-static"], 1),
            ([], 42),
        ],
    )
    def test_metadata_reply_roundtrip(self, static_bodies, stream_size):
        buf = encoder_protocol.pack_metadata_reply(static_bodies, stream_size)
        status = encoder_protocol.parse_status(
            buf[: encoder_protocol.REPLY_STATUS_SIZE]
        )
        body_start = encoder_protocol.REPLY_STATUS_SIZE
        body = buf[body_start:]
        assert status == len(body)
        n_static, got_stream_size = encoder_protocol.parse_metadata_prefix(
            body[: encoder_protocol.META_PREFIX_SIZE]
        )
        assert n_static == len(static_bodies)
        assert got_stream_size == stream_size
        sizes_start = encoder_protocol.META_PREFIX_SIZE
        sizes_end = sizes_start + n_static * encoder_protocol.META_SIZE_ENTRY_SIZE
        sizes = encoder_protocol.parse_static_sizes(
            body[sizes_start:sizes_end], n_static
        )
        assert sizes == tuple(len(b) for b in static_bodies)
        offset = sizes_end
        for body_bytes, body_size in zip(static_bodies, sizes, strict=True):
            assert body[offset : offset + body_size] == body_bytes
            offset += body_size
        assert offset == len(body)

    def test_read_reply_roundtrip(self):
        data = b"hello, world\x00\x01\x02"
        buf = encoder_protocol.pack_read_reply(data)
        status = encoder_protocol.parse_status(
            buf[: encoder_protocol.REPLY_STATUS_SIZE]
        )
        assert status == len(data)
        assert buf[encoder_protocol.REPLY_STATUS_SIZE :] == data

    def test_read_reply_empty(self):
        buf = encoder_protocol.pack_read_reply(b"")
        assert encoder_protocol.parse_status(buf) == 0
        assert len(buf) == encoder_protocol.REPLY_STATUS_SIZE


class TestStatusToError:
    def test_negative_status_becomes_oserror(self):
        err = encoder_protocol.status_to_error(-errno.EBADF)
        assert isinstance(err, OSError)
        assert err.errno == errno.EBADF

    def test_non_negative_status_rejected(self):
        with pytest.raises(ValueError, match="negative"):
            encoder_protocol.status_to_error(0)
        with pytest.raises(ValueError, match="negative"):
            encoder_protocol.status_to_error(7)


class TestErrnoForException:
    def test_oserror_errno_propagates(self):
        exc = OSError(errno.ENOENT, "missing")
        assert encoder_protocol.errno_for_exception(exc) == errno.ENOENT

    def test_arbitrary_exception_falls_back_to_eio(self):
        assert encoder_protocol.errno_for_exception(RuntimeError("boom")) == errno.EIO
