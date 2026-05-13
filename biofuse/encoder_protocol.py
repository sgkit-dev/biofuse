"""Wire protocol for the encoder-server subprocess.

A minimal request/reply protocol exchanged over a connected
``AF_UNIX`` ``SOCK_STREAM`` socket. Each socket carries one
synchronous conversation: the parent sends a request, awaits the
reply, then sends the next request. There is no ``seq`` id and no
``fh`` field — the *socket is the channel*.

Two request shapes
------------------

``M`` (1 byte) — get metadata. No payload.
    Reply body: ``<n_static:H><stream_size:Q>`` then
    ``n_static`` × ``<size:Q>`` then the concatenated static-file
    bodies in the order declared by the format spec. ``status`` on
    success is the total body length in bytes.

``R off:Q size:Q`` (17 bytes) — read ``size`` bytes at ``off`` from
    the connection's streaming file. Reply body is the data bytes;
    ``status`` is the body length.

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
# Variable-arity metadata reply prefix: number of static files plus the
# streaming-file size. Per-static sizes follow as a packed array of
# ``<Q>`` entries, then the concatenated static bodies.
_META_PREFIX = struct.Struct("<HQ")
_META_SIZE_ENTRY = struct.Struct("<Q")

REPLY_STATUS_SIZE = _REPLY_STATUS.size  # 8
REQ_READ_PAYLOAD_SIZE = _REQ_READ.size  # 16
META_PREFIX_SIZE = _META_PREFIX.size  # 10
META_SIZE_ENTRY_SIZE = _META_SIZE_ENTRY.size  # 8


# -- request encoding / decoding ----------------------------------------


def pack_get_metadata_request() -> bytes:
    return TAG_GET_METADATA


def pack_read_request(off: int, size: int) -> bytes:
    return TAG_READ + _REQ_READ.pack(off, size)


def parse_read_payload(buf: bytes) -> tuple[int, int]:
    """Decode the 16-byte READ payload into ``(off, size)``."""
    return _REQ_READ.unpack(buf)


# -- reply encoding -----------------------------------------------------


def pack_metadata_reply(static_bodies: list[bytes], stream_size: int) -> bytes:
    """Pack a variable-arity metadata reply.

    ``static_bodies`` is the per-static-file payload, in the spec's
    declared order. The reply body is the prefix + per-static sizes +
    the concatenated bodies; ``status`` is the body length.
    """
    n_static = len(static_bodies)
    prefix = _META_PREFIX.pack(n_static, stream_size)
    sizes = b"".join(_META_SIZE_ENTRY.pack(len(b)) for b in static_bodies)
    body = prefix + sizes + b"".join(static_bodies)
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


def parse_metadata_prefix(buf: bytes) -> tuple[int, int]:
    """Decode the ``<n_static, stream_size>`` prefix of a metadata reply."""
    return _META_PREFIX.unpack(buf)


def parse_static_sizes(buf: bytes, n_static: int) -> tuple[int, ...]:
    """Decode the per-static-file size array following the prefix."""
    return tuple(
        _META_SIZE_ENTRY.unpack_from(buf, offset=i * META_SIZE_ENTRY_SIZE)[0]
        for i in range(n_static)
    )


# -- shared helpers -----------------------------------------------------


def status_to_error(status: int) -> OSError:
    """Build an ``OSError`` from a negative reply status."""
    if status >= 0:
        raise ValueError(f"status must be negative (got {status})")
    err = -status
    return OSError(err, f"encoder-server reported errno {err}")


def errno_for_exception(exc: BaseException) -> int:
    """Pick a positive errno for any exception raised in the server.

    ``OSError`` is honoured directly; everything else becomes ``EIO``.
    """
    if isinstance(exc, OSError) and exc.errno is not None:
        return exc.errno
    return errno.EIO
