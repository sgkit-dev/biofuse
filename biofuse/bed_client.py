"""Parent-side async client for the BedEncoder worker subprocess.

:class:`BedEncoderClient` owns the worker subprocess lifecycle and
exposes ``open / read / release`` to :class:`PlinkOps` over the wire
protocol defined in :mod:`biofuse.bed_protocol`.

Lifecycle
---------

The client is constructed *before* the FUSE mount, while we are still in
sync code:

1. ``__init__`` spawns the worker via ``multiprocessing.get_context("spawn")``,
   sets up an ``AF_UNIX`` ``SOCK_STREAM`` socketpair, and synchronously
   issues a ``LIST`` request to obtain the file entries (sizes, modes).
2. ``PlinkOps`` reads the file entries and populates its inode map.
3. The FUSE mount starts; pyfuse3's main loop runs on a background thread
   under ``trio.run``. On the first async request the client lazily wraps
   the parent socket fd in a ``trio.lowlevel.FdStream``.
4. On shutdown, ``close()`` is called from sync context after the trio
   loop has terminated. It half-closes the connection so the worker
   sees EOF, then joins the subprocess.

The protocol is single-flight: a ``trio.Lock`` serialises requests so a
failure on one in-flight call cannot corrupt the next frame.
"""

import errno
import logging
import multiprocessing as mp
import os
import socket
import threading

import trio

from biofuse import bed_protocol, bed_worker

logger = logging.getLogger(__name__)


_SHUTDOWN_GRACE_SECONDS = 5.0


class BedEncoderClient:
    """Parent-side async client over a worker subprocess.

    Parameters
    ----------
    vcz_url
        The VCZ store URL, passed verbatim to ``vcztools.cli.make_reader``
        in the worker.
    basename
        Stem for the three exposed files.
    backend_storage
        Optional ``vcztools`` backend-storage selector. ``None`` lets
        ``make_reader`` choose its default.
    """

    def __init__(
        self,
        vcz_url: str,
        basename: str,
        *,
        backend_storage: str | None = None,
    ) -> None:
        ctx = mp.get_context("spawn")
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
        self._init_from_socket(parent_sock, proc)

    @classmethod
    def _for_test(cls, parent_sock: socket.socket, proc) -> "BedEncoderClient":
        """Construct a client around a preconnected socket+process.

        Test seam: lets tests pair the client with a ``serve()`` running
        in a thread on the other end of a ``socket.socketpair`` instead
        of a real subprocess. ``proc`` only needs to expose
        ``is_alive()``, ``join(timeout)``, ``terminate()``, ``kill()``.
        """
        instance = cls.__new__(cls)
        instance._init_from_socket(parent_sock, proc)
        return instance

    def _init_from_socket(self, parent_sock: socket.socket, proc) -> None:
        self._proc = proc
        self._parent_sock: socket.socket | None = parent_sock
        # Snapshot the fd so close() can release it via os.close() even
        # after the socket has been detached for the trio FdStream.
        self._fd: int | None = parent_sock.fileno()
        self._stream: trio.lowlevel.FdStream | None = None
        self._lock = trio.Lock()
        self._sync_lock = threading.Lock()
        self._closed = False
        try:
            self._file_entries: list[bed_protocol.FileSpec] = self._handshake_list()
        except BaseException:
            self.close()
            raise

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
            stream = await self._ensure_stream()
            await stream.send_all(request)
            status_buf = await _recv_exact_async(
                stream, bed_protocol.REPLY_STATUS_SIZE
            )
            status = bed_protocol.parse_status(status_buf)
            if status < 0:
                raise bed_protocol.status_to_error(status)
            if status == 0:
                return b""
            return await _recv_exact_async(stream, status)

    async def release(self, handle: int) -> None:
        """Release ``handle`` in the worker."""
        request = bed_protocol.pack_close_request(handle)
        status, _ = await self._roundtrip(request, body_size=0)
        if status < 0:
            raise bed_protocol.status_to_error(status)

    def close(self) -> None:
        """Tear down the worker subprocess. Idempotent. Sync-callable.

        Sends EOF to the worker by shutting the socket down, joins the
        process for a short grace period, then escalates to SIGTERM and
        SIGKILL if it refuses to exit.
        """
        with self._sync_lock:
            if self._closed:
                return
            self._closed = True
            self._send_eof_to_worker()
            self._parent_sock = None

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

    def _send_eof_to_worker(self) -> None:
        """Half-close the connection so the worker's recv returns 0.

        If no FdStream has been created yet, ``_parent_sock`` still owns
        the fd and we can close it directly. Otherwise the fd is owned
        by the FdStream; we ``os.dup()`` it, shut the dup down, and
        close the dup. The original fd stays valid for the FdStream's
        garbage-collection cleanup, so we don't trigger the spurious
        EBADF that an outright ``os.close()`` would cause.
        """
        if self._parent_sock is not None:
            try:
                self._parent_sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._parent_sock.close()
            return
        if self._fd is None:
            return
        try:
            dup_fd = os.dup(self._fd)
        except OSError as exc:
            logger.debug("os.dup on bed-worker fd raised: %s", exc)
            return
        try:
            with socket.socket(fileno=dup_fd) as wrap:
                try:
                    wrap.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        except OSError as exc:
            logger.debug("shutdown on bed-worker fd raised: %s", exc)

    # -- internals -------------------------------------------------------

    async def _roundtrip(
        self, request: bytes, *, body_size: int
    ) -> tuple[int, bytes]:
        async with self._lock:
            stream = await self._ensure_stream()
            await stream.send_all(request)
            status_buf = await _recv_exact_async(
                stream, bed_protocol.REPLY_STATUS_SIZE
            )
            status = bed_protocol.parse_status(status_buf)
            if status < 0:
                return status, b""
            if body_size == 0:
                return status, b""
            body = await _recv_exact_async(stream, body_size)
            return status, body

    async def _ensure_stream(self) -> trio.lowlevel.FdStream:
        if self._stream is not None:
            return self._stream
        if self._closed or self._parent_sock is None:
            raise OSError(errno.EIO, "bed-worker client is closed")
        # detach() returns the same fd integer we already snapshotted in
        # self._fd; the socket is now invalid but the fd is owned by
        # FdStream. close() will release it via os.close(self._fd).
        fd = self._parent_sock.detach()
        self._parent_sock = None
        self._stream = trio.lowlevel.FdStream(fd)
        return self._stream

    # -- synchronous handshake ------------------------------------------

    def _handshake_list(self) -> list[bed_protocol.FileSpec]:
        assert self._parent_sock is not None
        sock = self._parent_sock
        sock.sendall(bed_protocol.pack_list_request())
        status_buf = _recv_exact_sync(sock, bed_protocol.REPLY_STATUS_SIZE)
        status = bed_protocol.parse_status(status_buf)
        if status < 0:
            raise bed_protocol.status_to_error(status)
        entries: list[bed_protocol.FileSpec] = []
        for _ in range(status):
            hdr = _recv_exact_sync(sock, bed_protocol.REPLY_LIST_ENTRY_HDR_SIZE)
            name_len, size, mode = bed_protocol.parse_list_entry_header(hdr)
            name_bytes = _recv_exact_sync(sock, name_len) if name_len > 0 else b""
            entries.append(
                bed_protocol.FileSpec(name_bytes.decode("utf-8"), size, mode)
            )
        return entries


def _recv_exact_sync(sock: socket.socket, n: int) -> bytes:
    if n == 0:
        return b""
    buf = bytearray(n)
    view = memoryview(buf)
    got = 0
    while got < n:
        chunk = sock.recv_into(view[got:], n - got)
        if chunk == 0:
            raise EOFError("worker closed socket during handshake")
        got += chunk
    return bytes(buf)


async def _recv_exact_async(stream: trio.lowlevel.FdStream, n: int) -> bytes:
    """Read exactly ``n`` bytes off ``stream``, with a guaranteed checkpoint.

    Loops over ``stream.receive_some``; raises ``OSError(EIO)`` on EOF,
    which the caller surfaces to the FUSE layer as ``FUSEError(EIO)``.
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
