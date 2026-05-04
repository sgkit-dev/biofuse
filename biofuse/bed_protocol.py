"""Wire protocol for the BedEncoder worker subprocess.

A small request/reply protocol exchanged over a connected ``AF_UNIX``
``SOCK_STREAM`` socketpair. Single-flight: the parent serialises one
request at a time. Multi-byte fields are little-endian.

Frame shapes
------------

Request: a single ``<B`` tag byte followed by a tag-specific payload.

============  =========================================================
Tag           Payload
============  =========================================================
``b'L'``      (none) — list files
``b'O'``      ``<I`` name_len, then ``name_len`` bytes of UTF-8 name
``b'R'``      ``<QQQ`` handle, offset, size
``b'C'``      ``<Q`` handle
============  =========================================================

Reply: an ``<q`` *status* int64, then a status-and-tag-dependent payload.

- ``status < 0`` is an error: ``-status`` is the POSIX errno, no payload
  follows.
- ``status >= 0`` is success; the meaning of ``status`` and any trailing
  payload depend on the original request tag:

  - ``L``: ``status`` = number of entries; followed by N copies of
    ``<IQI`` (name_len, size, mode), each immediately followed by
    ``name_len`` bytes of UTF-8 name.
  - ``O``: ``status`` = 0 (sentinel); followed by ``<QQI`` (handle,
    size, mode).
  - ``R``: ``status`` = number of data bytes; followed by ``status``
    bytes of payload.
  - ``C``: ``status`` = 0; no trailing payload.

The module exports pure ``pack_*`` and ``parse_*`` functions plus a
small ``recv_exact`` helper used by both sync and async transports.
The functions never touch a socket: the transport reads enough bytes
for the next frame fragment and hands them off here.
"""

import errno
import logging
import struct
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# Request tags. Single-byte ASCII so framing is human-debuggable.
TAG_LIST = b"L"
TAG_OPEN = b"O"
TAG_READ = b"R"
TAG_CLOSE = b"C"

# Fixed header / payload sizes precomputed to avoid magic numbers in the
# transport code.
_REQ_OPEN_HDR = struct.Struct("<I")
_REQ_READ = struct.Struct("<QQQ")
_REQ_CLOSE = struct.Struct("<Q")

_REPLY_STATUS = struct.Struct("<q")
_REPLY_LIST_ENTRY_HDR = struct.Struct("<IQI")
_REPLY_OPEN_BODY = struct.Struct("<QQI")

REPLY_STATUS_SIZE = _REPLY_STATUS.size  # 8
REQ_READ_PAYLOAD_SIZE = _REQ_READ.size  # 24
REQ_CLOSE_PAYLOAD_SIZE = _REQ_CLOSE.size  # 8
REQ_OPEN_HDR_SIZE = _REQ_OPEN_HDR.size  # 4
REPLY_OPEN_BODY_SIZE = _REPLY_OPEN_BODY.size  # 20
REPLY_LIST_ENTRY_HDR_SIZE = _REPLY_LIST_ENTRY_HDR.size  # 16


@dataclass(frozen=True)
class FileSpec:
    """Description of a file the worker can serve.

    Mirrors ``biofuse.view.FileEntry`` but lives in the protocol layer to
    keep that layer free of intra-package imports.
    """

    name: str
    size: int
    mode: int


# -- request encoding ----------------------------------------------------


def pack_list_request() -> bytes:
    return TAG_LIST


def pack_open_request(name: str) -> bytes:
    name_bytes = name.encode("utf-8")
    return TAG_OPEN + _REQ_OPEN_HDR.pack(len(name_bytes)) + name_bytes


def pack_read_request(handle: int, offset: int, size: int) -> bytes:
    return TAG_READ + _REQ_READ.pack(handle, offset, size)


def pack_close_request(handle: int) -> bytes:
    return TAG_CLOSE + _REQ_CLOSE.pack(handle)


# -- reply encoding ------------------------------------------------------


def pack_error_reply(err: int) -> bytes:
    """Encode a negative-status error reply for any request type.

    ``err`` is a positive errno; it is sent as ``-err`` so the parent's
    sign check works uniformly.
    """
    if err <= 0:
        raise ValueError(f"err must be a positive errno (got {err})")
    return _REPLY_STATUS.pack(-err)


def pack_list_reply(entries: list[FileSpec]) -> bytes:
    parts = [_REPLY_STATUS.pack(len(entries))]
    for spec in entries:
        name_bytes = spec.name.encode("utf-8")
        parts.append(_REPLY_LIST_ENTRY_HDR.pack(len(name_bytes), spec.size, spec.mode))
        parts.append(name_bytes)
    return b"".join(parts)


def pack_open_reply(handle: int, size: int, mode: int) -> bytes:
    return _REPLY_STATUS.pack(0) + _REPLY_OPEN_BODY.pack(handle, size, mode)


def pack_read_reply(data: bytes) -> bytes:
    return _REPLY_STATUS.pack(len(data)) + data


def pack_close_reply() -> bytes:
    return _REPLY_STATUS.pack(0)


# -- request decoding (worker side) --------------------------------------


def parse_open_payload(buf: bytes) -> str:
    """Parse the bytes that follow the ``O`` tag plus its length header.

    The transport is expected to first read 4 bytes (the length header),
    then read that many bytes and pass them here.
    """
    return buf.decode("utf-8")


def parse_open_length_header(buf: bytes) -> int:
    (name_len,) = _REQ_OPEN_HDR.unpack(buf)
    return name_len


def parse_read_payload(buf: bytes) -> tuple[int, int, int]:
    return _REQ_READ.unpack(buf)


def parse_close_payload(buf: bytes) -> int:
    (handle,) = _REQ_CLOSE.unpack(buf)
    return handle


# -- reply decoding (parent side) ----------------------------------------


def parse_status(buf: bytes) -> int:
    (status,) = _REPLY_STATUS.unpack(buf)
    return status


def parse_open_body(buf: bytes) -> tuple[int, int, int]:
    return _REPLY_OPEN_BODY.unpack(buf)


def parse_list_entry_header(buf: bytes) -> tuple[int, int, int]:
    return _REPLY_LIST_ENTRY_HDR.unpack(buf)


def status_to_error(status: int) -> OSError:
    """Build an ``OSError`` from a negative reply status."""
    if status >= 0:
        raise ValueError(f"status must be negative (got {status})")
    err = -status
    return OSError(err, f"worker reported errno {err}")


def errno_for_exception(exc: BaseException) -> int:
    """Pick a positive errno for any exception raised in the worker.

    ``OSError`` is honoured directly; everything else becomes ``EIO``.
    """
    if isinstance(exc, OSError) and exc.errno is not None:
        return exc.errno
    return errno.EIO
