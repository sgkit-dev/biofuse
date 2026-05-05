"""Parent-side async client for the BedEncoder worker subprocess.

:class:`BedEncoderClient` owns the worker subprocess lifecycle and
exposes ``open / read / release`` to :class:`PlinkOps` over the wire
protocol defined in :mod:`biofuse.bed_protocol`.

The client is pipelined: each request carries an opaque ``seq`` and a
single background reader task demuxes replies back to the awaiting
caller. Multiple in-flight requests share the socket without a global
RPC lock — only the byte-level ``send_all`` is serialised so frames
on the wire stay intact. This lets the worker (which threads its
``serve()`` loop) actually run requests concurrently.

The client is fully trio-native: construction, RPCs, and shutdown all
run inside a single ``trio.run`` scope (the CLI's main loop). The
parent socket is wrapped as a :class:`trio.SocketStream` from the
moment the worker is spawned.
"""

import errno
import itertools
import logging
import multiprocessing as mp
import socket

import trio

from biofuse import bed_protocol, bed_worker

logger = logging.getLogger(__name__)


_SHUTDOWN_GRACE_SECONDS = 5.0


class _PendingCall:
    """One in-flight RPC: holds the awaiting caller's reply slot.

    The reader task sets ``body`` (the reply payload, possibly empty)
    or ``error`` (an exception built from a negative status, or a
    transport error) and then triggers ``event``. The caller awaits
    ``event`` and reads whichever field was populated.
    """

    __slots__ = ("event", "body", "error")

    def __init__(self) -> None:
        self.event = trio.Event()
        self.body: bytes | None = None
        self.error: BaseException | None = None


class BedEncoderClient:
    """Parent-side async client over a worker subprocess.

    Construct via :meth:`BedEncoderClient.connect` (an async classmethod
    that spawns the worker, performs the LIST handshake, and returns
    the ready client). The instance is also an async context manager:
    ``async with await BedEncoderClient.connect(...) as client: ...``
    will call :meth:`aclose` on exit.
    """

    @classmethod
    async def connect(
        cls,
        vcz_url: str,
        *,
        backend_storage: str | None = None,
    ) -> "BedEncoderClient":
        """Spawn the worker, do the LIST handshake, return a ready client."""
        ctx = mp.get_context("spawn")
        # socket.socketpair is a ~10 us kernel allocation (not I/O);
        # proc.start() blocks for ~10 ms on fork+exec+bootstrap. Both
        # are fine sync here: connect() is one-time setup, run before
        # the FUSE mount is live, so no concurrent trio task needs the
        # loop. Wrap proc.start() in trio.to_thread.run_sync only if
        # we ever start spawning workers from inside a busy loop.
        parent_sock, child_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            proc: mp.process.BaseProcess = ctx.Process(
                target=bed_worker._worker_main,
                args=(child_sock, vcz_url, backend_storage),
                name="biofuse-bed-worker",
            )
            proc.start()
        finally:
            # multiprocessing's socket reduction has duplicated the fd
            # into the child; the parent's copy of the child end can go.
            child_sock.close()
        stream = trio.SocketStream(trio.socket.from_stdlib_socket(parent_sock))
        return await cls._from_stream(stream, proc)

    @classmethod
    async def _from_stream(cls, stream: trio.SocketStream, proc) -> "BedEncoderClient":
        """Construct a client around a preconnected stream + process.

        Test seam: lets tests pair the client with a ``serve()`` running
        in a thread on the other end of a ``socket.socketpair`` instead
        of a real subprocess. ``proc`` only needs to expose
        ``is_alive()``, ``join(timeout)``, ``terminate()``, ``kill()``.
        """
        self = cls.__new__(cls)
        self._stream = stream
        self._proc = proc
        self._send_lock = trio.Lock()
        self._seq_counter = itertools.count(1)
        self._pending: dict[int, _PendingCall] = {}
        self._closed = False
        # Long-lived reader task lives in a nursery whose async-context
        # we drive manually around the client's lifetime: started here,
        # exited in aclose(). This lets the synchronous-looking client
        # API (``async with await connect(...)``) hold a background
        # task without forcing every caller to provide a nursery.
        self._reader_cm = trio.open_nursery()
        self._reader_nursery = await self._reader_cm.__aenter__()
        try:
            reader_started = trio.Event()
            self._reader_nursery.start_soon(self._reader_loop, reader_started)
            await reader_started.wait()
            self._file_entries = await self._handshake_list()
        except BaseException:
            await self.aclose()
            raise
        return self

    # -- async context manager ------------------------------------------

    async def __aenter__(self) -> "BedEncoderClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    # -- public API ------------------------------------------------------

    @property
    def file_entries(self) -> dict[bed_protocol.FileType, int]:
        """Mapping of ``FileType`` → size in bytes for each served file."""
        return dict(self._file_entries)

    async def open(self, fh: int, file_type: bed_protocol.FileType) -> None:
        """Allocate ``fh`` in the worker as a backend of ``file_type``.

        The caller (PlinkOps) owns the fh space and is responsible for
        not reusing in-use handles.
        """
        seq = next(self._seq_counter)
        request = bed_protocol.pack_open_request(seq, fh, file_type)
        await self._roundtrip(seq, request)

    async def read(self, fh: int, offset: int, size: int) -> bytes:
        """Read ``size`` bytes at ``offset`` for ``fh``."""
        seq = next(self._seq_counter)
        request = bed_protocol.pack_read_request(seq, fh, offset, size)
        body = await self._roundtrip(seq, request)
        return body

    async def release(self, fh: int) -> None:
        """Release ``fh`` in the worker."""
        seq = next(self._seq_counter)
        request = bed_protocol.pack_close_request(seq, fh)
        await self._roundtrip(seq, request)

    async def aclose(self) -> None:
        """Tear down the worker subprocess. Idempotent.

        Half-closes the write side so the worker's ``recv`` loop sees
        EOF and exits, then fully closes the stream and joins the
        subprocess. Escalates to SIGTERM and SIGKILL if the worker
        refuses to exit within the grace period.
        """
        if self._closed:
            return
        self._closed = True
        try:
            await self._stream.send_eof()
        except (trio.ClosedResourceError, trio.BrokenResourceError, OSError) as exc:
            logger.debug("send_eof on bed-worker stream raised: %s", exc)
        await self._stream.aclose()
        # The reader task exits once its receive_some sees EOF or the
        # stream is closed under it. Exiting the nursery waits for it.
        try:
            await self._reader_cm.__aexit__(None, None, None)
        except BaseException as exc:  # noqa: BLE001 - log and continue cleanup
            logger.debug("reader nursery exit raised: %s", exc)
        self._fail_all_pending(
            OSError(errno.EIO, "bed-worker client closed before reply")
        )
        await trio.to_thread.run_sync(self._sync_join_proc)

    # -- internals -------------------------------------------------------

    async def _roundtrip(self, seq: int, request: bytes) -> bytes:
        """Send ``request`` (already containing ``seq``) and await the reply.

        Returns the reply body bytes (possibly empty). Raises
        :class:`OSError` if the worker replied with a negative status,
        or if the connection failed before a reply arrived.
        """
        if self._closed:
            raise OSError(errno.EIO, "bed-worker client is closed")
        pending = _PendingCall()
        self._pending[seq] = pending
        try:
            async with self._send_lock:
                await self._stream.send_all(request)
            await pending.event.wait()
            if pending.error is not None:
                raise pending.error
            assert pending.body is not None
            return pending.body
        finally:
            self._pending.pop(seq, None)

    async def _reader_loop(self, started: trio.Event) -> None:
        """Background task: read replies and dispatch to pending callers.

        Treats ``status >= 0`` as a payload byte length; the parent
        :meth:`_handshake_list` parses LIST entries out of those bytes.
        ``status < 0`` is a negative-errno error, which is set on the
        pending entry as an :class:`OSError`.
        """
        started.set()
        try:
            while True:
                try:
                    header = await _recv_exact(
                        self._stream, bed_protocol.REPLY_HEADER_SIZE
                    )
                except (
                    OSError,
                    trio.ClosedResourceError,
                    trio.BrokenResourceError,
                ):
                    return
                seq, status = bed_protocol.parse_reply_header(header)
                pending = self._pending.get(seq)
                if status < 0:
                    if pending is not None:
                        pending.error = bed_protocol.status_to_error(status)
                        pending.event.set()
                    continue
                body = b""
                if status > 0:
                    try:
                        body = await _recv_exact(self._stream, status)
                    except (
                        OSError,
                        trio.ClosedResourceError,
                        trio.BrokenResourceError,
                    ) as exc:
                        if pending is not None:
                            pending.error = exc
                            pending.event.set()
                        return
                if pending is not None:
                    pending.body = body
                    pending.event.set()
        finally:
            self._fail_all_pending(OSError(errno.EIO, "bed-worker closed socket"))

    async def _handshake_list(self) -> dict[bed_protocol.FileType, int]:
        seq = next(self._seq_counter)
        body = await self._roundtrip(seq, bed_protocol.pack_list_request(seq))
        entry_size = bed_protocol.REPLY_LIST_ENTRY_SIZE
        if len(body) % entry_size != 0:
            raise OSError(
                errno.EIO,
                f"LIST reply body has {len(body)} bytes (not multiple of {entry_size})",
            )
        entries: dict[bed_protocol.FileType, int] = {}
        for off in range(0, len(body), entry_size):
            file_type, size = bed_protocol.parse_list_entry(
                body[off : off + entry_size]
            )
            entries[file_type] = size
        return entries

    def _fail_all_pending(self, exc: BaseException) -> None:
        for pending in list(self._pending.values()):
            if pending.error is None and pending.body is None:
                pending.error = exc
                pending.event.set()

    def _sync_join_proc(self) -> None:
        if self._proc.is_alive():
            self._proc.join(timeout=_SHUTDOWN_GRACE_SECONDS)
        if self._proc.is_alive():
            logger.warning("bed-worker did not exit; sending SIGTERM")
            self._proc.terminate()
            self._proc.join(timeout=_SHUTDOWN_GRACE_SECONDS)
        if self._proc.is_alive():
            logger.warning("bed-worker still alive after SIGTERM; killing")
            self._proc.kill()
            self._proc.join(timeout=_SHUTDOWN_GRACE_SECONDS)


async def _recv_exact(stream: trio.SocketStream, n: int) -> bytes:
    """Read exactly ``n`` bytes off ``stream``, with a guaranteed checkpoint.

    Raises :class:`OSError(EIO)` on EOF, which the caller surfaces to
    the FUSE layer as ``FUSEError(EIO)``.
    """
    if n == 0:
        await trio.lowlevel.checkpoint()
        return b""
    buf = bytearray(n)
    view = memoryview(buf)
    got = 0
    while got < n:
        chunk = await stream.receive_some(n - got)
        if len(chunk) == 0:
            raise OSError(errno.EIO, "bed-worker closed socket")
        view[got : got + len(chunk)] = chunk
        got += len(chunk)
    return bytes(buf)
