"""Mount a ``pyfuse3.Operations`` as a real FUSE mount.

pyfuse3 keeps mount state in module-level globals (``pyfuse3.init`` /
``pyfuse3.main`` / ``pyfuse3.close``), so only one biofuse mount can
be active per process at a time. The :class:`Mount` context manager
enforces this implicitly by being the only entry point.
"""

import logging
import shutil
import subprocess
import threading

import pyfuse3
import trio

logger = logging.getLogger(__name__)


class Mount:
    """Context manager that mounts a pyfuse3.Operations via pyfuse3.

    Mounts on ``__enter__``, runs the FUSE main loop on a background thread,
    and unmounts cleanly on ``__exit__``.

    The caller owns the lifecycle of the operations object: ``Mount`` does
    not construct or close it.

    pyfuse3's mount state is process-global, so only one Mount may be live
    in a given Python process at a time.
    """

    _global_lock = threading.Lock()
    _active: "Mount | None" = None

    def __init__(
        self,
        operations: pyfuse3.Operations,
        mountpoint: str,
        *,
        fsname: str = "biofuse",
        debug_fuse: bool = False,
    ) -> None:
        self._operations = operations
        self._mountpoint = mountpoint
        self._fsname = fsname
        self._debug_fuse = debug_fuse
        self._thread: threading.Thread | None = None
        self._exception: BaseException | None = None
        self._closed = False

    def __enter__(self) -> str:
        if not Mount._global_lock.acquire(blocking=False):
            raise RuntimeError(
                "another biofuse Mount is active in this process; "
                "pyfuse3 supports only one mount at a time"
            )
        try:
            options = set(pyfuse3.default_options)
            options.add(f"fsname={self._fsname}")
            options.add("ro")
            if self._debug_fuse:
                options.add("debug")
            pyfuse3.init(self._operations, self._mountpoint, options)
        except BaseException:
            Mount._global_lock.release()
            raise

        self._thread = threading.Thread(
            target=self._run, name="biofuse-fuse-main", daemon=True
        )
        self._thread.start()
        Mount._active = self
        return self._mountpoint

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _run(self) -> None:
        try:
            trio.run(pyfuse3.main)
        except BaseException as e:
            self._exception = e

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            try:
                pyfuse3.terminate()
            except Exception as exc:
                logger.debug("pyfuse3.terminate raised: %s", exc)
            # pyfuse3.terminate only marks the loop for exit; the loop is
            # blocked in the kernel waiting for the next request, so it does
            # not actually exit until the kernel sends one. Run fusermount3
            # -u to unmount (which generates an UMOUNT request and wakes the
            # loop). The lazy flag (-z) ensures the call returns even if
            # processes still hold open file handles.
            _force_unmount(self._mountpoint)
            if self._thread is not None:
                self._thread.join(timeout=15)
                if self._thread.is_alive():
                    logger.warning("FUSE main loop did not terminate within 15s")
            try:
                pyfuse3.close(unmount=False)
            except Exception as exc:
                logger.debug("pyfuse3.close raised: %s", exc)
        finally:
            Mount._active = None
            Mount._global_lock.release()
        if self._exception is not None:
            raise self._exception


def _force_unmount(mountpoint: str) -> None:
    """Run fusermount3 -z -u on mountpoint, ignoring failures."""
    fusermount = shutil.which("fusermount3") or shutil.which("fusermount")
    if fusermount is None:
        logger.warning("no fusermount executable found; cannot unmount %s", mountpoint)
        return
    try:
        subprocess.run(
            [fusermount, "-z", "-u", mountpoint],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=10,
        )
    except subprocess.TimeoutExpired:
        logger.warning("fusermount3 -u %s timed out", mountpoint)
