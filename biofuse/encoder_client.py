"""Parent-side async client for the encoder-server subprocess.

:class:`EncoderClient` connects briefly at startup to fetch the
precomputed static-sidecar bytes (in the format spec's declared
order) and the streaming-file size, then for each streaming-file
``open()`` from the FUSE layer it spins up a fresh
:class:`StreamConnection` over a new ``AF_UNIX`` socket. Each
:class:`StreamConnection` is its own conversation with its own
server-side thread and encoder.
"""

import errno
import logging
import multiprocessing as mp
import pathlib
import socket
import time
from collections.abc import Callable

import trio
from vcztools import cli as vcztools_cli

from biofuse import encoder_protocol, encoder_server, formats

logger = logging.getLogger(__name__)


_SHUTDOWN_GRACE_SECONDS = 5.0
_CONNECT_RETRY_SLEEP_S = 0.05
_CONNECT_DEADLINE_S = 10.0

# Per-operation deadlines for the parent → server protocol. The FUSE
# handler must never await indefinitely on the worker; on expiry we
# surface an ``OSError`` to the FUSE layer so the kernel sees a real
# I/O error and unblocks the consumer's syscall instead of pinning it
# in uninterruptible sleep.
_REQUEST_TIMEOUT_S = 30.0
_OPEN_TIMEOUT_S = 5.0
_ACLOSE_TIMEOUT_S = 2.0


class StreamConnection:
    """One streaming-file reader: a dedicated socket to the encoder-server.

    Reads on the same connection are serialised via an internal
    ``trio.Lock`` because the wire protocol is request/reply
    synchronous on a single socket. Different :class:`StreamConnection`
    instances are fully independent — they live in different server
    threads and do not contend with each other.
    """

    def __init__(
        self,
        stream: trio.SocketStream,
        *,
        on_aclose: Callable[[float, float], None] | None = None,
    ) -> None:
        self._stream = stream
        self._lock = trio.Lock()
        self._closed = False
        self._on_aclose = on_aclose

    async def read(self, off: int, size: int) -> bytes:
        if self._closed:
            raise OSError(errno.EIO, "stream connection is closed")
        request = encoder_protocol.pack_read_request(off, size)
        with trio.move_on_after(_REQUEST_TIMEOUT_S) as cs:
            async with self._lock:
                if self._closed:
                    raise OSError(errno.EIO, "stream connection is closed")
                await self._stream.send_all(request)
                status_buf = await _recv_exact(
                    self._stream, encoder_protocol.REPLY_STATUS_SIZE
                )
                status = encoder_protocol.parse_status(status_buf)
                if status < 0:
                    raise encoder_protocol.status_to_error(status)
                if status == 0:
                    return b""
                return await _recv_exact(self._stream, status)
        # Reached only if ``move_on_after`` caught a Cancelled — the
        # inner block always returns or raises through. Mark the
        # connection dead so other tasks queued on ``self._lock`` wake
        # to an immediate EIO instead of repeating the wait against a
        # known-broken socket.
        if not cs.cancelled_caught:  # pragma: no cover - defensive
            raise RuntimeError("encoder-server read fall-through")
        self._closed = True
        with trio.CancelScope(shield=True):
            with trio.move_on_after(_ACLOSE_TIMEOUT_S):
                try:
                    await self._stream.aclose()
                except (trio.BrokenResourceError, OSError) as exc:
                    logger.debug("aclose after timeout raised: %s", exc)
        raise OSError(errno.EIO, "encoder-server request timed out")

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        t_start = time.monotonic()
        with trio.CancelScope(shield=True):
            with trio.move_on_after(_ACLOSE_TIMEOUT_S) as cs:
                try:
                    await self._stream.send_eof()
                except (
                    trio.ClosedResourceError,
                    trio.BrokenResourceError,
                    OSError,
                ) as exc:
                    logger.debug("send_eof on stream connection raised: %s", exc)
                await self._stream.aclose()
            if cs.cancelled_caught:
                logger.debug(
                    "stream connection aclose timed out after %.1fs",
                    _ACLOSE_TIMEOUT_S,
                )
        if self._on_aclose is not None:
            try:
                self._on_aclose(t_start, time.monotonic())
            except Exception as exc:  # noqa: BLE001 - never let logging blow up cleanup
                logger.debug("on_aclose hook raised: %s", exc)


class EncoderClient:
    """Parent-side client for one mounted encoder-server subprocess.

    Construct via :meth:`EncoderClient.start` (an async classmethod
    that spawns the server, runs the metadata handshake, and returns
    the ready client). The instance is also an async context manager;
    ``aclose()`` signals the server to stop, joins the subprocess,
    and unlinks the listener socket file.

    Attributes populated by the handshake:

    - ``static_files``: dict mapping each suffix declared in
      ``spec.static_suffixes`` to its precomputed bytes.
    - ``stream_size``: total byte size of the streaming file.
    """

    def __init__(self, spec: formats.FormatSpec) -> None:
        self.spec = spec
        self.static_files: dict[str, bytes] = {}
        self.stream_size: int = 0
        self._proc: mp.process.BaseProcess | None = None
        self._socket_path: pathlib.Path | None = None
        self._stop_sock: socket.socket | None = None
        self._closed = False

    @classmethod
    async def start(
        cls,
        vcz_url: str,
        socket_path: pathlib.Path,
        spec: formats.FormatSpec,
        *,
        reader_options: vcztools_cli.ViewPlinkOptions | None = None,
        log_config: vcztools_cli.LogConfig | None = None,
    ) -> "EncoderClient":
        """Spawn the server, run the metadata handshake, return client.

        The parent creates the listener and the stop-signal socketpair
        itself, then hands both to the child. Multiprocessing's socket
        reduction dups the fds across the spawn boundary; the parent
        closes its own copies once the child has started.

        ``reader_options`` carries the bcftools-view-style filtering
        options forwarded to vcztools' ``make_reader`` in the worker;
        ``log_config`` configures logging in the worker so its
        ``logger.debug`` / ``info`` output reaches the parent's sink.
        Both default to the empty / WARNING configuration.
        """
        if reader_options is None:
            reader_options = vcztools_cli.ViewPlinkOptions()
        if log_config is None:
            log_config = vcztools_cli.LogConfig()
        socket_path = pathlib.Path(socket_path)
        socket_path.parent.mkdir(parents=True, exist_ok=True)
        if socket_path.exists():
            socket_path.unlink()
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(socket_path))
        listener.listen(64)
        parent_stop, child_stop = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        ctx = mp.get_context("spawn")
        proc: mp.process.BaseProcess = ctx.Process(
            target=encoder_server._server_main,
            args=(listener, child_stop, vcz_url, spec, reader_options, log_config),
            name=f"biofuse-{spec.name}-server",
        )
        try:
            proc.start()
        finally:
            listener.close()
            child_stop.close()

        self = cls(spec)
        self._proc = proc
        self._socket_path = socket_path
        self._stop_sock = parent_stop
        try:
            await self._handshake()
        except BaseException as exc:
            # If the subprocess has exited on its own during startup
            # the handshake failure is a downstream symptom (refused
            # connect, RST mid frame, premature EOF). Surface a single
            # clean OSError so the CLI prints one error line and
            # points the user at the server's own log. Check
            # ``is_alive`` before ``aclose`` — the latter signals stop
            # and joins the child, after which ``exitcode`` is set
            # regardless of why the child exited.
            child_died_on_own = not proc.is_alive()
            await self.aclose()
            if child_died_on_own and not isinstance(exc, KeyboardInterrupt):
                raise OSError(
                    errno.EIO,
                    f"{spec.name}-server exited during startup; "
                    "see log above for details",
                ) from exc
            raise
        return self

    async def __aenter__(self) -> "EncoderClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def open_stream(
        self,
        *,
        on_aclose: Callable[[float, float], None] | None = None,
    ) -> StreamConnection:
        """Open a new dedicated socket for one streaming-file reader."""
        stream = await self._connect_stream()
        return StreamConnection(stream, on_aclose=on_aclose)

    async def aclose(self) -> None:
        """Tear down the server. Idempotent.

        Closes the parent end of the stop-signal socketpair so the
        server's accept loop wakes via ``select`` and exits, joins
        the subprocess (escalating to SIGTERM / SIGKILL after
        ``_SHUTDOWN_GRACE_SECONDS``), and unlinks the listener path.
        """
        if self._closed:
            return
        self._closed = True
        if self._stop_sock is not None:
            try:
                self._stop_sock.close()
            except OSError:
                pass
            self._stop_sock = None
        if self._proc is not None:
            await trio.to_thread.run_sync(self._sync_join_proc)
        if self._socket_path is not None and self._socket_path.exists():
            try:
                self._socket_path.unlink()
            except OSError as exc:
                logger.debug("unlink socket path raised: %s", exc)

    # -- internals ------------------------------------------------------

    async def _handshake(self) -> None:
        stream = await self._connect_stream(retry_until_listening=True)
        try:
            await stream.send_all(encoder_protocol.pack_get_metadata_request())
            status_buf = await _recv_exact(stream, encoder_protocol.REPLY_STATUS_SIZE)
            status = encoder_protocol.parse_status(status_buf)
            if status < 0:
                raise encoder_protocol.status_to_error(status)
            prefix = await _recv_exact(stream, encoder_protocol.META_PREFIX_SIZE)
            n_static, stream_size = encoder_protocol.parse_metadata_prefix(prefix)
            if n_static != len(self.spec.static_suffixes):
                raise OSError(
                    errno.EIO,
                    f"{self.spec.name}-server reported {n_static} static files; "
                    f"client expected {len(self.spec.static_suffixes)}",
                )
            sizes_buf = await _recv_exact(
                stream, n_static * encoder_protocol.META_SIZE_ENTRY_SIZE
            )
            sizes = encoder_protocol.parse_static_sizes(sizes_buf, n_static)
            self.static_files = {
                suffix: await _recv_exact(stream, size)
                for suffix, size in zip(self.spec.static_suffixes, sizes, strict=True)
            }
            self.stream_size = stream_size
        finally:
            try:
                await stream.send_eof()
            except (
                trio.ClosedResourceError,
                trio.BrokenResourceError,
                OSError,
            ):
                pass
            await stream.aclose()

    async def _connect_stream(
        self, *, retry_until_listening: bool = False
    ) -> trio.SocketStream:
        """Open a fresh ``AF_UNIX`` connection to the server.

        With ``retry_until_listening=True`` the call retries briefly
        while the child is still bringing up its accept loop, bounded
        by ``_CONNECT_DEADLINE_S``. If the child has already exited
        (typically due to a startup failure that ``_server_main``
        caught and logged) the retry loop bails out immediately
        instead of waiting out the deadline.
        """
        assert self._socket_path is not None
        path = str(self._socket_path)
        deadline = trio.current_time() + _CONNECT_DEADLINE_S
        last_exc: BaseException | None = None
        while True:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.setblocking(False)
            trio_sock = trio.socket.from_stdlib_socket(sock)
            try:
                with trio.fail_after(_OPEN_TIMEOUT_S):
                    await trio_sock.connect(path)
                return trio.SocketStream(trio_sock)
            except trio.TooSlowError as exc:
                trio_sock.close()
                raise OSError(
                    errno.EIO,
                    f"encoder-server connect timed out after {_OPEN_TIMEOUT_S:.1f}s",
                ) from exc
            except (FileNotFoundError, ConnectionRefusedError, OSError) as exc:
                trio_sock.close()
                last_exc = exc
                if not retry_until_listening:
                    raise
                if self._proc is not None and not self._proc.is_alive():
                    raise OSError(
                        errno.EIO,
                        f"{self.spec.name}-server exited during startup; "
                        "see log above for details",
                    ) from last_exc
                if trio.current_time() > deadline:
                    raise OSError(
                        errno.EIO,
                        f"{self.spec.name}-server not listening at {path}: {exc}",
                    ) from last_exc
                await trio.sleep(_CONNECT_RETRY_SLEEP_S)

    def _sync_join_proc(self) -> None:
        proc = self._proc
        if proc is None:
            return
        if proc.is_alive():
            proc.join(timeout=_SHUTDOWN_GRACE_SECONDS)
        if proc.is_alive():
            logger.warning("%s-server did not exit; sending SIGTERM", self.spec.name)
            proc.terminate()
            proc.join(timeout=_SHUTDOWN_GRACE_SECONDS)
        if proc.is_alive():
            logger.warning(
                "%s-server still alive after SIGTERM; killing", self.spec.name
            )
            proc.kill()
            proc.join(timeout=_SHUTDOWN_GRACE_SECONDS)


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
            raise OSError(errno.EIO, "encoder-server closed socket")
        view[got : got + len(chunk)] = chunk
        got += len(chunk)
    return bytes(buf)
