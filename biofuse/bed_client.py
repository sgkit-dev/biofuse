"""Parent-side async client for the BedEncoder worker subprocess.

:class:`BedEncoderClient` owns the worker subprocess lifecycle and
exposes ``open / read / release`` to :class:`PlinkOps` over the wire
protocol defined in :mod:`biofuse.bed_protocol`.

The client is fully trio-native: construction, RPCs, and shutdown all
run inside a single ``trio.run`` scope (the CLI's main loop). The
parent socket is wrapped as a :class:`trio.SocketStream` from the
moment the worker is spawned; the LIST handshake, request/reply
roundtrips, and graceful shutdown (``send_eof`` → ``aclose``) all use
trio primitives.

The protocol is single-flight: a :class:`trio.Lock` serialises
requests so a failure on one in-flight call cannot corrupt the next
frame.
"""

import errno
import logging
import multiprocessing as mp
import socket

import trio

from biofuse import bed_protocol, bed_worker

logger = logging.getLogger(__name__)


_SHUTDOWN_GRACE_SECONDS = 5.0


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
        basename: str,
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
        parent_sock, child_sock = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_STREAM
        )
        try:
            proc: mp.process.BaseProcess = ctx.Process(
                target=bed_worker._worker_main,
                args=(child_sock, vcz_url, basename, backend_storage),
                name=f"biofuse-bed-worker[{basename}]",
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
        self._lock = trio.Lock()
        self._closed = False
        try:
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
    def file_entries(self) -> list[bed_protocol.FileSpec]:
        return list(self._file_entries)

    async def open(self, name: str) -> tuple[int, int, int]:
        """Open ``name`` in the worker. Returns ``(handle, size, mode)``."""
        request = bed_protocol.pack_open_request(name)
        status, body = await self._roundtrip(
            request, body_size=bed_protocol.REPLY_OPEN_BODY_SIZE
        )
        if status < 0:
            raise bed_protocol.status_to_error(status)
        return bed_protocol.parse_open_body(body)

    async def read(self, handle: int, offset: int, size: int) -> bytes:
        """Read ``size`` bytes at ``offset`` for ``handle``."""
        request = bed_protocol.pack_read_request(handle, offset, size)
        async with self._lock:
            await self._stream.send_all(request)
            status_buf = await _recv_exact(
                self._stream, bed_protocol.REPLY_STATUS_SIZE
            )
            status = bed_protocol.parse_status(status_buf)
            if status < 0:
                raise bed_protocol.status_to_error(status)
            if status == 0:
                return b""
            return await _recv_exact(self._stream, status)

    async def release(self, handle: int) -> None:
        """Release ``handle`` in the worker."""
        request = bed_protocol.pack_close_request(handle)
        status, _ = await self._roundtrip(request, body_size=0)
        if status < 0:
            raise bed_protocol.status_to_error(status)

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
        await trio.to_thread.run_sync(self._sync_join_proc)

    # -- internals -------------------------------------------------------

    async def _roundtrip(
        self, request: bytes, *, body_size: int
    ) -> tuple[int, bytes]:
        async with self._lock:
            await self._stream.send_all(request)
            status_buf = await _recv_exact(
                self._stream, bed_protocol.REPLY_STATUS_SIZE
            )
            status = bed_protocol.parse_status(status_buf)
            if status < 0:
                return status, b""
            if body_size == 0:
                return status, b""
            body = await _recv_exact(self._stream, body_size)
            return status, body

    async def _handshake_list(self) -> list[bed_protocol.FileSpec]:
        await self._stream.send_all(bed_protocol.pack_list_request())
        status_buf = await _recv_exact(
            self._stream, bed_protocol.REPLY_STATUS_SIZE
        )
        status = bed_protocol.parse_status(status_buf)
        if status < 0:
            raise bed_protocol.status_to_error(status)
        entries: list[bed_protocol.FileSpec] = []
        for _ in range(status):
            hdr = await _recv_exact(
                self._stream, bed_protocol.REPLY_LIST_ENTRY_HDR_SIZE
            )
            name_len, size, mode = bed_protocol.parse_list_entry_header(hdr)
            name_bytes = (
                await _recv_exact(self._stream, name_len) if name_len > 0 else b""
            )
            entries.append(
                bed_protocol.FileSpec(name_bytes.decode("utf-8"), size, mode)
            )
        return entries

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
