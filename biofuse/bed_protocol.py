"""Wire protocol for the BedEncoder worker subprocess.

A small request/reply protocol exchanged over a connected ``AF_UNIX``
``SOCK_STREAM`` socketpair. Each frame carries an opaque caller-assigned
``seq_id`` so the parent can pipeline multiple in-flight requests and
demux replies that arrive out of order. Multi-byte fields are little-
endian.

The worker treats file handles as opaque integers assigned by the
parent (PlinkOps). Filenames never cross the wire — PlinkOps maps
filenames to a small :class:`FileType` tag and that's all the worker
sees.

Frame shapes
------------

Request: ``<seq:Q>`` then a single ``<B>`` tag byte then a tag-specific
payload.

============  =========================================================
Tag           Payload
============  =========================================================
``b'L'``      (none) — list files
``b'O'``      ``<QB`` (fh, file_type)
``b'R'``      ``<QQQ`` (fh, offset, size)
``b'C'``      ``<Q`` (fh)
============  =========================================================

Reply: ``<seq:Q>`` then ``<status:q>`` then a status-and-tag-dependent
payload. The reply ``seq`` is the caller-assigned ``seq`` from the
matching request, echoed back verbatim.

- ``status < 0`` is an error: ``-status`` is the POSIX errno, no payload
  follows.
- ``status >= 0`` is success; on success ``status`` is the size of the
  trailing payload in bytes (i.e. the parent always reads exactly
  ``status`` more bytes off the socket after the header). The shape of
  those bytes depends on the original request tag:

  - ``L``: ``status`` = total payload length in bytes; the payload is
    a tightly packed array of ``<BQ`` (file_type, size) records.
  - ``O``: ``status`` = 0; no trailing payload.
  - ``R``: ``status`` = number of data bytes; payload is the data.
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
_SEQ = struct.Struct("<Q")
_REQ_OPEN = struct.Struct("<QB")
_REQ_READ = struct.Struct("<QQQ")
_REQ_CLOSE = struct.Struct("<Q")
_REPLY_STATUS = struct.Struct("<q")
_REPLY_LIST_ENTRY = struct.Struct("<BQ")

SEQ_SIZE = _SEQ.size  # 8
REQ_HEADER_SIZE = _SEQ.size + 1  # seq + tag = 9
REPLY_HEADER_SIZE = _SEQ.size + _REPLY_STATUS.size  # seq + status = 16
REQ_OPEN_PAYLOAD_SIZE = _REQ_OPEN.size  # 9
REQ_READ_PAYLOAD_SIZE = _REQ_READ.size  # 24
REQ_CLOSE_PAYLOAD_SIZE = _REQ_CLOSE.size  # 8
REPLY_LIST_ENTRY_SIZE = _REPLY_LIST_ENTRY.size  # 9


# -- request encoding ----------------------------------------------------


def pack_list_request(seq: int) -> bytes:
    return _SEQ.pack(seq) + TAG_LIST


def pack_open_request(seq: int, fh: int, file_type: FileType) -> bytes:
    return _SEQ.pack(seq) + TAG_OPEN + _REQ_OPEN.pack(fh, int(file_type))


def pack_read_request(seq: int, fh: int, offset: int, size: int) -> bytes:
    return _SEQ.pack(seq) + TAG_READ + _REQ_READ.pack(fh, offset, size)


def pack_close_request(seq: int, fh: int) -> bytes:
    return _SEQ.pack(seq) + TAG_CLOSE + _REQ_CLOSE.pack(fh)


# -- reply encoding ------------------------------------------------------


def pack_error_reply(seq: int, err: int) -> bytes:
    """Encode a negative-status error reply for any request type."""
    if err <= 0:
        raise ValueError(f"err must be a positive errno (got {err})")
    return _SEQ.pack(seq) + _REPLY_STATUS.pack(-err)


def pack_list_reply(seq: int, entries: list[tuple[FileType, int]]) -> bytes:
    body = b"".join(
        _REPLY_LIST_ENTRY.pack(int(file_type), size) for file_type, size in entries
    )
    return _SEQ.pack(seq) + _REPLY_STATUS.pack(len(body)) + body


def pack_open_reply(seq: int) -> bytes:
    return _SEQ.pack(seq) + _REPLY_STATUS.pack(0)


def pack_read_reply(seq: int, data: bytes) -> bytes:
    return _SEQ.pack(seq) + _REPLY_STATUS.pack(len(data)) + data


def pack_close_reply(seq: int) -> bytes:
    return _SEQ.pack(seq) + _REPLY_STATUS.pack(0)


# -- request decoding (worker side) --------------------------------------


def parse_seq(buf: bytes) -> int:
    (seq,) = _SEQ.unpack(buf)
    return seq


def parse_open_payload(buf: bytes) -> tuple[int, FileType]:
    fh, file_type_raw = _REQ_OPEN.unpack(buf)
    return fh, FileType(file_type_raw)


def parse_read_payload(buf: bytes) -> tuple[int, int, int]:
    return _REQ_READ.unpack(buf)


def parse_close_payload(buf: bytes) -> int:
    (fh,) = _REQ_CLOSE.unpack(buf)
    return fh


# -- reply decoding (parent side) ----------------------------------------


def parse_reply_header(buf: bytes) -> tuple[int, int]:
    """Parse the ``<seq><status>`` reply header. ``buf`` must be exactly
    :data:`REPLY_HEADER_SIZE` bytes."""
    seq = _SEQ.unpack(buf[: _SEQ.size])[0]
    status = _REPLY_STATUS.unpack(buf[_SEQ.size :])[0]
    return seq, status


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
