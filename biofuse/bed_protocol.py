"""Wire protocol for the BedEncoder worker subprocess.

A small request/reply protocol exchanged over a connected ``AF_UNIX``
``SOCK_STREAM`` socketpair. Single-flight: the parent serialises one
request at a time. Multi-byte fields are little-endian.

The worker treats file handles as opaque integers assigned by the
parent (PlinkOps). Filenames never cross the wire — PlinkOps maps
filenames to a small :class:`FileType` tag and that's all the worker
sees.

Frame shapes
------------

Request: a single ``<B`` tag byte followed by a tag-specific payload.

============  =========================================================
Tag           Payload
============  =========================================================
``b'L'``      (none) — list files
``b'O'``      ``<QB`` (fh, file_type)
``b'R'``      ``<QQQ`` (fh, offset, size)
``b'C'``      ``<Q`` (fh)
============  =========================================================

Reply: an ``<q`` *status* int64, then a status-and-tag-dependent payload.

- ``status < 0`` is an error: ``-status`` is the POSIX errno, no payload
  follows.
- ``status >= 0`` is success; the meaning of ``status`` and any trailing
  payload depend on the original request tag:

  - ``L``: ``status`` = number of entries; followed by N copies of
    ``<BQ`` (file_type, size).
  - ``O``: ``status`` = 0; no trailing payload.
  - ``R``: ``status`` = number of data bytes; followed by ``status``
    bytes of payload.
  - ``C``: ``status`` = 0; no trailing payload.
"""

import enum
import errno
import logging
import struct

logger = logging.getLogger(__name__)


class FileType(enum.IntEnum):
    """The kind of file the worker is asked to serve.

    Encoded as a single byte on the wire. Values are stable so older
    code can interpret newer worker replies if more types are added
    later.
    """

    BED = 0
    BIM = 1
    FAM = 2


# Request tags. Single-byte ASCII so framing is human-debuggable.
TAG_LIST = b"L"
TAG_OPEN = b"O"
TAG_READ = b"R"
TAG_CLOSE = b"C"

# Fixed payload struct definitions and precomputed sizes.
_REQ_OPEN = struct.Struct("<QB")
_REQ_READ = struct.Struct("<QQQ")
_REQ_CLOSE = struct.Struct("<Q")
_REPLY_STATUS = struct.Struct("<q")
_REPLY_LIST_ENTRY = struct.Struct("<BQ")

REPLY_STATUS_SIZE = _REPLY_STATUS.size  # 8
REQ_OPEN_PAYLOAD_SIZE = _REQ_OPEN.size  # 9
REQ_READ_PAYLOAD_SIZE = _REQ_READ.size  # 24
REQ_CLOSE_PAYLOAD_SIZE = _REQ_CLOSE.size  # 8
REPLY_LIST_ENTRY_SIZE = _REPLY_LIST_ENTRY.size  # 9


# -- request encoding ----------------------------------------------------


def pack_list_request() -> bytes:
    return TAG_LIST


def pack_open_request(fh: int, file_type: FileType) -> bytes:
    return TAG_OPEN + _REQ_OPEN.pack(fh, int(file_type))


def pack_read_request(fh: int, offset: int, size: int) -> bytes:
    return TAG_READ + _REQ_READ.pack(fh, offset, size)


def pack_close_request(fh: int) -> bytes:
    return TAG_CLOSE + _REQ_CLOSE.pack(fh)


# -- reply encoding ------------------------------------------------------


def pack_error_reply(err: int) -> bytes:
    """Encode a negative-status error reply for any request type."""
    if err <= 0:
        raise ValueError(f"err must be a positive errno (got {err})")
    return _REPLY_STATUS.pack(-err)


def pack_list_reply(entries: list[tuple[FileType, int]]) -> bytes:
    parts = [_REPLY_STATUS.pack(len(entries))]
    for file_type, size in entries:
        parts.append(_REPLY_LIST_ENTRY.pack(int(file_type), size))
    return b"".join(parts)


def pack_open_reply() -> bytes:
    return _REPLY_STATUS.pack(0)


def pack_read_reply(data: bytes) -> bytes:
    return _REPLY_STATUS.pack(len(data)) + data


def pack_close_reply() -> bytes:
    return _REPLY_STATUS.pack(0)


# -- request decoding (worker side) --------------------------------------


def parse_open_payload(buf: bytes) -> tuple[int, FileType]:
    fh, file_type_raw = _REQ_OPEN.unpack(buf)
    return fh, FileType(file_type_raw)


def parse_read_payload(buf: bytes) -> tuple[int, int, int]:
    return _REQ_READ.unpack(buf)


def parse_close_payload(buf: bytes) -> int:
    (fh,) = _REQ_CLOSE.unpack(buf)
    return fh


# -- reply decoding (parent side) ----------------------------------------


def parse_status(buf: bytes) -> int:
    (status,) = _REPLY_STATUS.unpack(buf)
    return status


def parse_list_entry(buf: bytes) -> tuple[FileType, int]:
    file_type_raw, size = _REPLY_LIST_ENTRY.unpack(buf)
    return FileType(file_type_raw), size


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
