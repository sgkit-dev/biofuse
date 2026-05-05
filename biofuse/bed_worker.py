"""Worker-side logic for the BedEncoder subprocess.

Three layers, each independently testable:

- :class:`WorkerSession` is pure logic. It owns the ``VczReader``,
  the precomputed ``.bim``/``.fam`` bytes, and a table of open handles
  ({fh -> :class:`vcztools.plink.BedEncoder` or
  :class:`_StaticBytesFile`}). The worker never sees filenames; the
  parent (PlinkOps) assigns each fh and tags it with a
  :class:`bed_protocol.FileType`. No sockets, no subprocess: tests
  can construct it directly. Thread-safe: ``open``/``release``/``close``
  serialise on a session lock; ``read`` serialises on a per-fh lock so
  concurrent reads on different fhs run in parallel while same-fh
  reads stay safe (``BedEncoder.read`` is not reentrant).

- :func:`serve` is a multi-threaded request/reply loop. The calling
  thread reads framed requests off a socket and routes each one:
  LIST/OPEN go to a shared thread pool, while READ/CLOSE on a given
  ``fh`` go to a per-fh FIFO queue feeding the same pool. Per-fh
  FIFO is load-bearing: ``BedEncoder`` decodes streamingly, so
  out-of-order reads on a single fh trigger a full re-decode from a
  chunk boundary. Different fhs are processed concurrently. Replies
  may interleave on the wire — every reply echoes back the caller-
  assigned ``seq`` from the request so the parent can demux.

- :func:`_worker_main` is the subprocess entry point used by
  :class:`BedEncoderClient`. It receives a ``socket.socket``
  (handle-passed by ``multiprocessing``), constructs a
  :class:`WorkerSession`, then runs :func:`serve`.

Only this module imports ``vcztools``. The parent FUSE process must
not import this module — that is what keeps the FUSE process free of
Zarr machinery.
"""

import collections
import concurrent.futures as cf
import dataclasses
import errno
import logging
import socket
import threading

from vcztools import cli as vcztools_cli
from vcztools import plink as vcztools_plink
from vcztools import retrieval as vcztools_retrieval

from biofuse import bed_protocol

logger = logging.getLogger(__name__)


_DEFAULT_WORKER_THREADS = 4


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

    Thread-safety
    -------------

    ``open``, ``release`` and ``close`` mutate the open-handle table
    under ``_session_lock``. ``read`` acquires only the small
    ``_session_lock`` to look the backend up, then drops it and
    acquires a per-fh ``threading.Lock`` for the duration of the
    backend call. Per-fh locks let reads on distinct fhs run in
    parallel while preventing concurrent calls into the same
    :class:`vcztools.plink.BedEncoder` (which is not reentrant).

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

        self._open_handles: dict[int, vcztools_plink.BedEncoder | _StaticBytesFile] = {}
        self._fh_locks: dict[int, threading.Lock] = {}
        self._session_lock = threading.Lock()

    def list_files(self) -> list[tuple[bed_protocol.FileType, int]]:
        return [
            (bed_protocol.FileType.BED, self._bed_size),
            (bed_protocol.FileType.BIM, len(self._bim_bytes)),
            (bed_protocol.FileType.FAM, len(self._fam_bytes)),
        ]

    def open(self, fh: int, file_type: bed_protocol.FileType) -> None:
        """Allocate a backend for ``fh``. Raises if ``fh`` is already open."""
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
        with self._session_lock:
            if fh in self._open_handles:
                # Drop the freshly-built backend; nothing else has touched it.
                try:
                    backend.close()
                except Exception as exc:  # noqa: BLE001 - best-effort cleanup
                    logger.debug("backend close on EEXIST raised: %s", exc)
                raise OSError(errno.EEXIST, f"handle {fh} already open")
            self._open_handles[fh] = backend
            self._fh_locks[fh] = threading.Lock()

    def read(self, fh: int, off: int, size: int) -> bytes:
        with self._session_lock:
            backend = self._open_handles.get(fh)
            fh_lock = self._fh_locks.get(fh)
        if backend is None or fh_lock is None:
            raise OSError(errno.EBADF, f"unknown handle {fh}")
        with fh_lock:
            return backend.read(off, size)

    def release(self, fh: int) -> None:
        with self._session_lock:
            backend = self._open_handles.pop(fh, None)
            fh_lock = self._fh_locks.pop(fh, None)
        if backend is None:
            return
        # Wait out any in-flight read on this fh before closing the backend.
        # ``fh_lock`` is paired with ``backend`` 1:1 in ``open``, so it is
        # always present here when ``backend`` is.
        with fh_lock:
            backend.close()

    def close(self) -> None:
        """Close every still-open backend. Called once on shutdown."""
        with self._session_lock:
            handles = list(self._open_handles.items())
            self._open_handles.clear()
            self._fh_locks.clear()
        for _, backend in handles:
            try:
                backend.close()
            except Exception as exc:  # noqa: BLE001 - best-effort cleanup
                logger.debug("backend close raised on shutdown: %s", exc)


# -- multi-threaded serve loop ------------------------------------------


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
) -> tuple[int, bytes, tuple[object, ...]] | None:
    """Read one framed request off ``sock``.

    Returns ``(seq, tag, args)`` where ``args`` is a tuple of decoded
    payload fields, or ``None`` on clean EOF before any header was
    read (parent has closed the socket — normal shutdown).
    """
    header = _recv_exact_sync(sock, bed_protocol.REQ_HEADER_SIZE)
    if len(header) == 0:
        return None
    seq = bed_protocol.parse_seq(header[: bed_protocol.SEQ_SIZE])
    tag = bytes(header[bed_protocol.SEQ_SIZE :])
    if tag == bed_protocol.TAG_LIST:
        return seq, tag, ()
    if tag == bed_protocol.TAG_OPEN:
        body = _recv_exact_sync(sock, bed_protocol.REQ_OPEN_PAYLOAD_SIZE)
        if len(body) < bed_protocol.REQ_OPEN_PAYLOAD_SIZE:
            raise EOFError("socket closed mid-OPEN payload")
        return seq, tag, bed_protocol.parse_open_payload(body)
    if tag == bed_protocol.TAG_READ:
        body = _recv_exact_sync(sock, bed_protocol.REQ_READ_PAYLOAD_SIZE)
        if len(body) < bed_protocol.REQ_READ_PAYLOAD_SIZE:
            raise EOFError("socket closed mid-READ payload")
        return seq, tag, bed_protocol.parse_read_payload(body)
    if tag == bed_protocol.TAG_CLOSE:
        body = _recv_exact_sync(sock, bed_protocol.REQ_CLOSE_PAYLOAD_SIZE)
        if len(body) < bed_protocol.REQ_CLOSE_PAYLOAD_SIZE:
            raise EOFError("socket closed mid-CLOSE payload")
        return seq, tag, (bed_protocol.parse_close_payload(body),)
    raise ValueError(f"unknown request tag: {tag!r}")


def _dispatch(
    session: WorkerSession, seq: int, tag: bytes, args: tuple[object, ...]
) -> bytes:
    if tag == bed_protocol.TAG_LIST:
        entries = session.list_files()
        return bed_protocol.pack_list_reply(seq, entries)
    if tag == bed_protocol.TAG_OPEN:
        fh, file_type = args
        session.open(fh, file_type)
        return bed_protocol.pack_open_reply(seq)
    if tag == bed_protocol.TAG_READ:
        fh, off, size = args
        data = session.read(fh, off, size)
        return bed_protocol.pack_read_reply(seq, data)
    if tag == bed_protocol.TAG_CLOSE:
        (fh,) = args
        session.release(fh)
        return bed_protocol.pack_close_reply(seq)
    raise ValueError(f"unknown tag in dispatch: {tag!r}")


def _handle_one(
    session: WorkerSession,
    sock: socket.socket,
    write_lock: threading.Lock,
    seq: int,
    tag: bytes,
    args: tuple[object, ...],
) -> None:
    """Execute one request and write its reply back on the socket.

    Errors raised by the session are translated to negative-status
    error replies. Exceptions are caught here so a single bad request
    cannot tear down the worker pool.
    """
    try:
        reply = _dispatch(session, seq, tag, args)
    except Exception as exc:  # noqa: BLE001 - any error becomes errno reply
        err = bed_protocol.errno_for_exception(exc)
        if not isinstance(exc, OSError):
            logger.exception("worker dispatch raised; replying with EIO")
        reply = bed_protocol.pack_error_reply(seq, err)
    try:
        with write_lock:
            sock.sendall(reply)
    except OSError as exc:
        logger.warning("worker send failed: %s", exc)


@dataclasses.dataclass
class _FhQueue:
    """Per-fh dispatch state.

    Only one request per fh is allowed in the thread pool at a time
    (``in_flight``). Further requests for the same fh wait in
    ``queue`` and are dispatched in arrival order once the previous
    one completes. This preserves the offset-sequential order
    ``BedEncoder`` requires to stay on its streaming fast path while
    still letting different fhs run concurrently in the pool.
    """

    in_flight: bool = False
    queue: collections.deque = dataclasses.field(default_factory=collections.deque)


def serve(
    sock: socket.socket,
    session: WorkerSession,
    *,
    max_workers: int = _DEFAULT_WORKER_THREADS,
) -> None:
    """Multi-threaded request/reply loop. Returns on clean EOF.

    The calling thread reads framed requests off ``sock`` and routes
    them: LIST/OPEN are submitted directly to the shared thread pool,
    while READ/CLOSE on a given ``fh`` are gated through a per-fh
    FIFO queue. At most one request per fh runs in the pool at a
    time; the rest wait their turn in the queue. Different fhs run
    concurrently. Replies may interleave on the wire — the parent
    demuxes by ``seq``. Framing errors (truncated requests, unknown
    tags) end the loop after waiting for in-flight work to drain.
    """
    write_lock = threading.Lock()
    pool = cf.ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix="bed-worker"
    )
    fh_queues: dict[int, _FhQueue] = {}
    fh_queues_lock = threading.Lock()

    def _run_per_fh(fh: int, seq: int, tag: bytes, args: tuple[object, ...]) -> None:
        try:
            _handle_one(session, sock, write_lock, seq, tag, args)
        finally:
            next_work = None
            with fh_queues_lock:
                state = fh_queues.get(fh)
                if state is not None:
                    if state.queue:
                        next_work = state.queue.popleft()
                    else:
                        state.in_flight = False
                        if tag == bed_protocol.TAG_CLOSE:
                            # No more requests will arrive for this fh
                            # (the parent serialises CLOSE after all
                            # READs and only opens new fhs through the
                            # parent-side fh allocator).
                            fh_queues.pop(fh, None)
            if next_work is not None:
                next_seq, next_tag, next_args = next_work
                pool.submit(_run_per_fh, fh, next_seq, next_tag, next_args)

    def _submit_fh(fh: int, seq: int, tag: bytes, args: tuple[object, ...]) -> None:
        with fh_queues_lock:
            state = fh_queues.setdefault(fh, _FhQueue())
            if state.in_flight:
                state.queue.append((seq, tag, args))
                return
            state.in_flight = True
        pool.submit(_run_per_fh, fh, seq, tag, args)

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
            seq, tag, args = msg
            if tag == bed_protocol.TAG_READ or tag == bed_protocol.TAG_CLOSE:
                fh = args[0]
                _submit_fh(fh, seq, tag, args)
            else:
                # LIST or OPEN: no per-fh ordering required.
                pool.submit(_handle_one, session, sock, write_lock, seq, tag, args)
    finally:
        pool.shutdown(wait=True)
        session.close()


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
