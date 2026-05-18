"""In-process host for the per-format encoders.

Replaces the previous ``encoder_client`` / ``encoder_server`` /
``encoder_protocol`` stack: the ``VczReader`` and the per-fh
:class:`~vcztools.format_encoder.FormatEncoder` instances live in the
FUSE handler process. Heavy blocking work (encoder construction,
``encoder.read``, ``encoder.close``, and reader teardown) is dispatched
to worker threads via :func:`trio.to_thread.run_sync` so the pyfuse3
trio task is free to schedule other FUSE requests.

:class:`EncoderHost` mirrors the public surface the FUSE Operations
layer expects (``static_files``, ``stream_size``, ``open_stream`` ->
:class:`StreamHandle`) so :class:`biofuse.encoder_ops.EncoderOps` is
agnostic to whether the encoder lives in-process or out-of-process.

Per-read timeout handling matters because the trio thread cannot kill
a worker thread. The read path uses ``abandon_on_cancel=True`` so the
trio task wakes immediately on timeout, but the worker thread keeps
running ``encoder.read`` to completion. :class:`StreamHandle` tracks
the thread via a ``threading.Event`` that the worker itself sets in a
``finally`` block, and :meth:`StreamHandle.aclose` drains that event
before calling ``encoder.close`` — concurrent close+read on the same
encoder is unsafe.
"""

import errno
import logging
import threading
import time
from collections.abc import Callable

import trio

from biofuse import formats

logger = logging.getLogger(__name__)


# Per-request deadline for ``encoder.read``. Slow reads under high I/O
# load do legitimately happen; surfacing EIO to the FUSE syscall is
# better than pinning the consumer in uninterruptible sleep.
_REQUEST_TIMEOUT_S = 30.0

# Outer deadline for ``StreamHandle.aclose`` — covers draining any
# abandoned worker thread plus the ``encoder.close`` call. If the
# abandoned thread is permanently wedged we log a warning and leak the
# encoder rather than hang unmount indefinitely.
_ACLOSE_TIMEOUT_S = 2.0


class StreamHandle:
    """One streaming-file reader.

    Owns one :class:`~vcztools.format_encoder.FormatEncoder`. Reads
    serialise on an internal ``trio.Lock`` — :class:`FormatEncoder`
    mutates iterator state in place, so at most one worker thread may
    enter ``encoder.read`` at a time. ``read`` and ``aclose``
    off-thread the blocking calls via :func:`trio.to_thread.run_sync`.

    On a per-read timeout the handle is marked closed and subsequent
    reads return ``OSError(EIO)`` immediately; the abandoned worker
    thread eventually returns, at which point :meth:`aclose` may close
    the encoder.
    """

    def __init__(
        self,
        encoder,
        *,
        on_aclose: Callable[[float, float], None] | None = None,
    ) -> None:
        self._encoder = encoder
        self._lock = trio.Lock()
        self._closed = False
        self._aclose_called = False
        self._on_aclose = on_aclose
        # Cleared on read entry, set by the worker thread on return.
        # Used by ``aclose`` to drain any abandoned worker before
        # closing the encoder. Initial state = idle.
        self._thread_done = threading.Event()
        self._thread_done.set()

    async def read(self, off: int, size: int) -> bytes:
        if self._closed:
            raise OSError(errno.EIO, "stream handle is closed")
        # Fast path: serve from the encoder's in-memory cache without
        # taking the lock or dispatching to a worker thread.
        # ``try_cached_read`` is documented thread-safe and never
        # advances the iterator, so it is safe to call concurrently
        # with an in-flight slow-path ``encoder.read`` on another task.
        cached = self._encoder.try_cached_read(off, size)
        if cached is not None:
            return cached
        async with self._lock:
            if self._closed:
                raise OSError(errno.EIO, "stream handle is closed")
            self._thread_done.clear()
            encoder = self._encoder
            thread_done = self._thread_done

            def call() -> bytes:
                try:
                    return encoder.read(off, size)
                finally:
                    thread_done.set()

            with trio.move_on_after(_REQUEST_TIMEOUT_S) as cs:
                try:
                    return await trio.to_thread.run_sync(call, abandon_on_cancel=True)
                except OSError:
                    raise
                except Exception as exc:
                    # Non-OSError encoder failures (``ValueError`` for
                    # haploid PLINK, ``NotImplementedError`` for mixed
                    # ploidy BGEN, …) become ``OSError(EIO)`` so the
                    # FUSE layer surfaces a real I/O error to the
                    # kernel rather than crashing the trio task.
                    logger.error("encoder.read raised; converting to EIO: %s", exc)
                    logger.debug("encoder.read traceback", exc_info=True)
                    raise OSError(errno.EIO, f"encoder.read failed: {exc}") from exc
        if cs.cancelled_caught:
            # Mark dead so subsequent reads on this fh return EIO
            # immediately rather than queueing behind a known-stuck
            # worker. The abandoned thread will eventually set
            # ``_thread_done``; ``aclose`` waits for it.
            self._closed = True
            raise OSError(errno.EIO, "encoder read timed out")
        raise RuntimeError("encoder read fall-through")  # pragma: no cover

    async def aclose(self) -> None:
        if self._aclose_called:
            return
        self._aclose_called = True
        self._closed = True
        t_start = time.monotonic()
        # Wait for any abandoned worker thread to finish before
        # calling ``encoder.close``. The encoder's own
        # ``ThreadPoolExecutor`` is shut down inside ``close``;
        # in-flight tasks must drain first. ``Event.wait`` carries
        # its own timeout because cancelling ``to_thread.run_sync``
        # from outside cannot interrupt a thread blocked on a
        # ``threading.Event``. ``abandon_on_cancel=True`` shields the
        # trio task from a still-running drain thread on shutdown.
        drained = await trio.to_thread.run_sync(
            self._thread_done.wait,
            _ACLOSE_TIMEOUT_S,
            abandon_on_cancel=True,
        )
        if drained:
            await trio.to_thread.run_sync(self._encoder.close, abandon_on_cancel=True)
        else:
            logger.warning(
                "encoder did not finish draining within %.1fs; "
                "leaking encoder and its thread pool",
                _ACLOSE_TIMEOUT_S,
            )
        if self._on_aclose is not None:
            try:
                self._on_aclose(t_start, time.monotonic())
            except Exception as exc:  # noqa: BLE001 - never let logging blow up cleanup
                logger.debug("on_aclose hook raised: %s", exc)


class EncoderHost:
    """Parent-process host for one mounted view.

    Construct via :meth:`EncoderHost.start` — an async classmethod
    that opens the reader, materialises the variant filter, and builds
    the static-sidecar bytes on a worker thread, returning a ready
    host. The instance is also an async context manager; ``aclose``
    closes the reader.

    Attributes populated by ``start``:

    - ``static_files``: dict mapping each suffix returned by
      ``spec.static_suffixes(opts)`` to its precomputed bytes.
    - ``stream_size``: total byte size of the streaming file.
    """

    def __init__(self, spec: formats.FormatSpec, opts) -> None:
        self.spec = spec
        self.opts = opts
        self.static_files: dict[str, bytes] = {}
        self.stream_size: int = 0
        self._reader = None
        self._closed = False

    @classmethod
    async def start(
        cls,
        vcz_url: str,
        spec: formats.FormatSpec,
        *,
        opts,
    ) -> "EncoderHost":
        """Open the reader and build the static-sidecar bytes."""
        self = cls(spec, opts)
        try:
            await trio.to_thread.run_sync(self._sync_start, vcz_url)
        except BaseException:
            await self.aclose()
            raise
        return self

    def _sync_start(self, vcz_url: str) -> None:
        reader = self.opts.make_reader(vcz_url)
        try:
            reader.materialise_variant_filter()
            expected_suffixes = self.spec.static_suffixes(self.opts)
            static_files = self.spec.build_static_files(reader, self.opts)
            missing = set(expected_suffixes) - set(static_files)
            extra = set(static_files) - set(expected_suffixes)
            if missing or extra:
                raise ValueError(
                    f"{self.spec.name}: build_static_files returned keys "
                    f"{sorted(static_files)}; expected {list(expected_suffixes)}"
                )
            with self.spec.encoder_factory(reader, self.opts) as encoder:
                stream_size = int(encoder.total_size)
        except BaseException:
            reader.__exit__(None, None, None)
            raise
        # Keep ordering in the static_files dict equal to the spec's
        # declared order — the FUSE adapter does not depend on this
        # but it keeps logs / diagnostics stable.
        self.static_files = {
            suffix: static_files[suffix] for suffix in expected_suffixes
        }
        self.stream_size = stream_size
        self._reader = reader

    async def __aenter__(self) -> "EncoderHost":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def open_stream(
        self,
        *,
        on_aclose: Callable[[float, float], None] | None = None,
    ) -> StreamHandle:
        """Construct a fresh encoder and wrap it in a :class:`StreamHandle`.

        Encoder construction runs in a worker thread because it
        touches the reader's metadata caches and spins up a
        ``ThreadPoolExecutor``.
        """
        if self._reader is None:
            raise OSError(errno.EIO, "encoder host is closed")
        encoder = await trio.to_thread.run_sync(self._sync_open_encoder)
        return StreamHandle(encoder, on_aclose=on_aclose)

    def _sync_open_encoder(self):
        cm = self.spec.encoder_factory(self._reader, self.opts)
        encoder = cm.__enter__()
        # We rely on ``encoder.close`` (called by ``StreamHandle.aclose``)
        # to release the encoder's resources; the context-manager
        # protocol on ``FormatEncoder`` is a thin wrapper around it.
        return encoder

    async def aclose(self) -> None:
        """Close the reader. Idempotent."""
        if self._closed:
            return
        self._closed = True
        reader = self._reader
        self._reader = None
        if reader is not None:
            await trio.to_thread.run_sync(reader.__exit__, None, None, None)
