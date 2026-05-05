"""Wire protocol for the plink-server subprocess.

A minimal request/reply protocol exchanged over a connected
``AF_UNIX`` ``SOCK_STREAM`` socket. Each socket carries one
synchronous conversation: the parent sends a request, awaits the
reply, then sends the next request. There is no ``seq`` id and no
``fh`` field — the *socket is the channel*.

Two request shapes
------------------

``M`` (1 byte) — get metadata. No payload.
    Reply body: ``<bim_size:Q><fam_size:Q><bed_size:Q>`` then
    ``bim_size`` bytes of ``.bim`` then ``fam_size`` bytes of
    ``.fam``. ``status`` on success is the body length in bytes
    (``= 24 + bim_size + fam_size``).

``R off:Q size:Q`` (17 bytes) — read ``size`` bytes at ``off`` from
    the connection's ``.bed`` view. The encoder is created lazily
    on the first ``R`` for this socket, so the metadata-handshake
    socket can be discarded without paying the encoder-build cost.
    Reply body is the data bytes; ``status`` is the body length.

Reply layout (common)
---------------------

``<status:q>`` followed by ``status`` bytes of body when
``status >= 0``. ``status < 0`` is an error reply: ``-status`` is a
POSIX errno and no body follows.
"""

import errno
import struct

TAG_GET_METADATA = b"M"
TAG_READ = b"R"

_REQ_READ = struct.Struct("<QQ")
_REPLY_STATUS = struct.Struct("<q")
_META_HEADER = struct.Struct("<QQQ")  # bim_size, fam_size, bed_size

REPLY_STATUS_SIZE = _REPLY_STATUS.size  # 8
REQ_READ_PAYLOAD_SIZE = _REQ_READ.size  # 16
META_HEADER_SIZE = _META_HEADER.size  # 24


# -- request encoding / decoding ----------------------------------------


def pack_get_metadata_request() -> bytes:
    return TAG_GET_METADATA


def pack_read_request(off: int, size: int) -> bytes:
    return TAG_READ + _REQ_READ.pack(off, size)


def parse_read_payload(buf: bytes) -> tuple[int, int]:
    """Decode the 16-byte READ payload into ``(off, size)``."""
    return _REQ_READ.unpack(buf)


# -- reply encoding -----------------------------------------------------


def pack_metadata_reply(bim_bytes: bytes, fam_bytes: bytes, bed_size: int) -> bytes:
    body = (
        _META_HEADER.pack(len(bim_bytes), len(fam_bytes), bed_size)
        + bim_bytes
        + fam_bytes
    )
    return _REPLY_STATUS.pack(len(body)) + body


def pack_read_reply(data: bytes) -> bytes:
    return _REPLY_STATUS.pack(len(data)) + data


def pack_error_reply(err: int) -> bytes:
    if err <= 0:
        raise ValueError(f"err must be a positive errno (got {err})")
    return _REPLY_STATUS.pack(-err)


# -- reply decoding -----------------------------------------------------


def parse_status(buf: bytes) -> int:
    (status,) = _REPLY_STATUS.unpack(buf)
    return status


def parse_metadata_header(buf: bytes) -> tuple[int, int, int]:
    """Decode the ``<bim_size, fam_size, bed_size>`` header at the
    front of a metadata reply body."""
    return _META_HEADER.unpack(buf)


# -- shared helpers -----------------------------------------------------


def status_to_error(status: int) -> OSError:
    """Build an ``OSError`` from a negative reply status."""
    if status >= 0:
        raise ValueError(f"status must be negative (got {status})")
    err = -status
    return OSError(err, f"plink-server reported errno {err}")


def errno_for_exception(exc: BaseException) -> int:
    """Pick a positive errno for any exception raised in the server.

    ``OSError`` is honoured directly; everything else becomes ``EIO``.
    """
    if isinstance(exc, OSError) and exc.errno is not None:
        return exc.errno
    return errno.EIO
