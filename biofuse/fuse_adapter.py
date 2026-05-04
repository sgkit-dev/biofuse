"""Mount a ``pyfuse3.Operations`` as a real FUSE mount.

pyfuse3 keeps mount state in module-level globals (``pyfuse3.init`` /
``pyfuse3.main`` / ``pyfuse3.close``), so only one biofuse mount can
be active per process at a time. The :func:`mount` async context
manager enforces this implicitly by being the only entry point.
"""

import logging
import shutil
import subprocess
import threading
from contextlib import asynccontextmanager

import pyfuse3
import trio

logger = logging.getLogger(__name__)


_global_lock = threading.Lock()


@asynccontextmanager
async def mount(
    operations: pyfuse3.Operations,
    mountpoint: str,
    *,
    fsname: str = "biofuse",
    debug_fuse: bool = False,
):
    """Mount ``operations`` at ``mountpoint`` and run pyfuse3.main.

    The pyfuse3 main loop runs as a child task in a trio nursery
    inside this manager's scope. Exiting the ``async with`` block
    terminates pyfuse3 and unmounts cleanly via fusermount3.
    """
    if not _global_lock.acquire(blocking=False):
        raise RuntimeError(
            "another biofuse mount is active in this process; "
            "pyfuse3 supports only one mount at a time"
        )
    try:
        options = set(pyfuse3.default_options)
        options.add(f"fsname={fsname}")
        options.add("ro")
        if debug_fuse:
            options.add("debug")
        pyfuse3.init(operations, mountpoint, options)
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(pyfuse3.main)
                try:
                    yield mountpoint
                finally:
                    try:
                        pyfuse3.terminate()
                    except Exception as exc:
                        logger.debug("pyfuse3.terminate raised: %s", exc)
                    # pyfuse3.terminate only flags the loop for exit;
                    # it does not actually wake the kernel-blocked
                    # main loop until the kernel sends a request.
                    # fusermount3 -u generates an UMOUNT request that
                    # wakes the loop. -z (lazy) returns even if
                    # processes still hold open file handles.
                    _force_unmount(mountpoint)
                # Nursery exit waits for pyfuse3.main to return.
        finally:
            try:
                pyfuse3.close(unmount=False)
            except Exception as exc:
                logger.debug("pyfuse3.close raised: %s", exc)
    finally:
        _global_lock.release()


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
