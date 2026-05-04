"""Worker-side logic for the BedEncoder subprocess.

Three layers, each independently testable:

- :class:`WorkerSession` is pure logic. It owns the ``VczReader``,
  the precomputed ``.bim``/``.fam`` bytes, and a table of open handles
  ({fh -> :class:`vcztools.plink.BedEncoder` or
  :class:`_StaticBytesFile`}). The worker never sees filenames; the
  parent (PlinkOps) assigns each fh and tags it with a
  :class:`bed_protocol.FileType`. No sockets, no subprocess: tests
  can construct it directly.

- :func:`serve` is a blocking sync loop that reads framed requests
  off a socket and writes framed replies. Tests can drive it via an
  in-process ``socket.socketpair`` running on a thread.

- :func:`_worker_main` is the subprocess entry point used by
  :class:`BedEncoderClient`. It receives a ``socket.socket``
  (handle-passed by ``multiprocessing``), constructs a
  :class:`WorkerSession`, then runs :func:`serve`.

Only this module imports ``vcztools``. The parent FUSE process must
not import this module — that is what keeps the FUSE process free of
Zarr machinery.
"""

import errno
import logging
import socket

from vcztools import cli as vcztools_cli
from vcztools import plink as vcztools_plink
from vcztools import retrieval as vcztools_retrieval

from biofuse import bed_protocol

logger = logging.getLogger(__name__)


class _StaticBytesFile:
    """Per-handle adapter for a file whose bytes are precomputed in memory.

    Implements the same ``read(off, size) -> bytes`` and ``close()``
    shape as :class:`vcztools.plink.BedEncoder` so
    :class:`WorkerSession` can dispatch uniformly.
    """

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._closed = False

    @property
    def size(self) -> int:
        return len(self._data)

    def read(self, off: int, size: int) -> bytes:
        if self._closed:
            raise RuntimeError("static file closed")
        if off < 0:
            raise ValueError(f"off must be >= 0 (got {off})")
        if size < 0:
            raise ValueError(f"size must be >= 0 (got {size})")
        if off >= len(self._data) or size == 0:
            return b""
        end = min(off + size, len(self._data))
        return bytes(self._data[off:end])

    def close(self) -> None:
        self._closed = True


class WorkerSession:
    """In-process logic for the bed-worker subprocess.

    Holds a single ``VczReader`` and the precomputed ``.bim``/``.fam``
    bytes. ``open(fh, file_type)`` materialises a backend keyed by the
    caller-assigned ``fh``: a fresh ``BedEncoder`` for
    :attr:`FileType.BED`, an in-memory bytes view for
    :attr:`FileType.BIM` / :attr:`FileType.FAM`. ``read`` and
    ``release`` look the backend up by ``fh``.

    Parameters
    ----------
    reader
        Already-constructed ``VczReader``. The caller owns its
        lifetime; ``WorkerSession`` does not close it.
    """

    def __init__(self, reader: vcztools_retrieval.VczReader) -> None:
        self._reader = reader

        bim_text = vcztools_plink.generate_bim(reader)
        fam_text = vcztools_plink.generate_fam(reader)
        self._bim_bytes = bim_text.encode("utf-8")
        self._fam_bytes = fam_text.encode("utf-8")

        num_variants = reader.num_variants
        num_samples = int(reader.sample_ids.size)
        bytes_per_variant = (num_samples + 3) // 4
        self._bed_size = 3 + num_variants * bytes_per_variant

        self._open_handles: dict[
            int, vcztools_plink.BedEncoder | _StaticBytesFile
        ] = {}

    def list_files(self) -> list[tuple[bed_protocol.FileType, int]]:
        return [
            (bed_protocol.FileType.BED, self._bed_size),
            (bed_protocol.FileType.BIM, len(self._bim_bytes)),
            (bed_protocol.FileType.FAM, len(self._fam_bytes)),
        ]

    def open(self, fh: int, file_type: bed_protocol.FileType) -> None:
        """Allocate a backend for ``fh``. Raises if ``fh`` is already open."""
        if fh in self._open_handles:
            raise OSError(errno.EEXIST, f"handle {fh} already open")
        if file_type == bed_protocol.FileType.BED:
            backend: vcztools_plink.BedEncoder | _StaticBytesFile = (
                vcztools_plink.BedEncoder(self._reader)
            )
        elif file_type == bed_protocol.FileType.BIM:
            backend = _StaticBytesFile(self._bim_bytes)
        elif file_type == bed_protocol.FileType.FAM:
            backend = _StaticBytesFile(self._fam_bytes)
        else:
            raise OSError(errno.EINVAL, f"unknown file_type {file_type}")
        self._open_handles[fh] = backend

    def read(self, fh: int, off: int, size: int) -> bytes:
        backend = self._open_handles.get(fh)
        if backend is None:
            raise OSError(errno.EBADF, f"unknown handle {fh}")
        return backend.read(off, size)

    def release(self, fh: int) -> None:
        backend = self._open_handles.pop(fh, None)
        if backend is None:
            return
        backend.close()

    def close(self) -> None:
        """Close every still-open backend. Called once on shutdown."""
        for backend in self._open_handles.values():
            try:
                backend.close()
            except Exception as exc:  # noqa: BLE001 - best-effort cleanup
                logger.debug("backend close raised on shutdown: %s", exc)
        self._open_handles.clear()


# -- blocking serve loop -------------------------------------------------


def _recv_exact_sync(sock: socket.socket, n: int) -> bytes:
    """Read exactly ``n`` bytes off ``sock``. Returns ``b""`` on clean EOF.

    Raises :class:`EOFError` if EOF arrives mid-frame (i.e. a partial
    request from the parent — protocol violation).
    """
    if n == 0:
        return b""
    buf = bytearray(n)
    view = memoryview(buf)
    got = 0
    while got < n:
        chunk = sock.recv_into(view[got:], n - got)
        if chunk == 0:
            if got == 0:
                return b""
            raise EOFError(f"socket closed mid-frame after {got}/{n} bytes")
        got += chunk
    return bytes(buf)


def _read_request(
    sock: socket.socket,
) -> tuple[bytes, tuple[object, ...]] | None:
    """Read one framed request off ``sock``.

    Returns ``(tag, args)`` where ``args`` is a tuple of decoded payload
    fields, or ``None`` on clean EOF before any tag was read (parent has
    closed the socket — normal shutdown).
    """
    tag_buf = _recv_exact_sync(sock, 1)
    if len(tag_buf) == 0:
        return None
    tag = bytes(tag_buf)
    if tag == bed_protocol.TAG_LIST:
        return tag, ()
    if tag == bed_protocol.TAG_OPEN:
        body = _recv_exact_sync(sock, bed_protocol.REQ_OPEN_PAYLOAD_SIZE)
        if len(body) < bed_protocol.REQ_OPEN_PAYLOAD_SIZE:
            raise EOFError("socket closed mid-OPEN payload")
        return tag, bed_protocol.parse_open_payload(body)
    if tag == bed_protocol.TAG_READ:
        body = _recv_exact_sync(sock, bed_protocol.REQ_READ_PAYLOAD_SIZE)
        if len(body) < bed_protocol.REQ_READ_PAYLOAD_SIZE:
            raise EOFError("socket closed mid-READ payload")
        return tag, bed_protocol.parse_read_payload(body)
    if tag == bed_protocol.TAG_CLOSE:
        body = _recv_exact_sync(sock, bed_protocol.REQ_CLOSE_PAYLOAD_SIZE)
        if len(body) < bed_protocol.REQ_CLOSE_PAYLOAD_SIZE:
            raise EOFError("socket closed mid-CLOSE payload")
        return tag, (bed_protocol.parse_close_payload(body),)
    raise ValueError(f"unknown request tag: {tag!r}")


def serve(sock: socket.socket, session: WorkerSession) -> None:
    """Blocking request/reply loop. Returns on clean EOF.

    Errors raised by the session are translated to negative-status
    error replies; the loop continues. Framing errors (truncated
    requests, unknown tags) are logged and the loop exits.
    """
    try:
        while True:
            try:
                msg = _read_request(sock)
            except EOFError as exc:
                logger.warning("worker frame error: %s", exc)
                return
            except ValueError as exc:
                logger.warning("worker protocol error: %s", exc)
                return
            if msg is None:
                return
            tag, args = msg
            try:
                reply = _dispatch(session, tag, args)
            except Exception as exc:  # noqa: BLE001 - any error becomes errno reply
                err = bed_protocol.errno_for_exception(exc)
                if not isinstance(exc, OSError):
                    logger.exception("worker dispatch raised; replying with EIO")
                reply = bed_protocol.pack_error_reply(err)
            try:
                sock.sendall(reply)
            except OSError as exc:
                logger.warning("worker send failed: %s", exc)
                return
    finally:
        session.close()


def _dispatch(
    session: WorkerSession, tag: bytes, args: tuple[object, ...]
) -> bytes:
    if tag == bed_protocol.TAG_LIST:
        entries = session.list_files()
        return bed_protocol.pack_list_reply(entries)
    if tag == bed_protocol.TAG_OPEN:
        fh, file_type = args
        session.open(fh, file_type)
        return bed_protocol.pack_open_reply()
    if tag == bed_protocol.TAG_READ:
        fh, off, size = args
        data = session.read(fh, off, size)
        return bed_protocol.pack_read_reply(data)
    if tag == bed_protocol.TAG_CLOSE:
        (fh,) = args
        session.release(fh)
        return bed_protocol.pack_close_reply()
    raise ValueError(f"unknown tag in dispatch: {tag!r}")


# -- subprocess entry point ----------------------------------------------


def _worker_main(
    sock: socket.socket,
    vcz_url: str,
    backend_storage: str | None,
) -> None:
    """Subprocess entry point started via ``multiprocessing.Process``.

    The socket is handle-passed by ``multiprocessing`` (its reduction
    machinery turns the parent-side ``socket`` argument into a fresh
    socket in the child pointing at the same underlying connection).

    Constructs a ``VczReader`` and a :class:`WorkerSession`, then runs
    :func:`serve`. Returns on socket EOF; the process exits normally
    on return.
    """
    reader = vcztools_cli.make_reader(vcz_url, backend_storage=backend_storage)
    session = WorkerSession(reader)
    try:
        serve(sock, session)
    finally:
        sock.close()
