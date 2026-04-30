"""pyfuse3 adapter that exposes a FilesystemView as a real FUSE mount.

The adapter is a thin shim: every FUSE operation is translated into a call
on the underlying view. The view does the real work; the adapter only
manages inode/handle bookkeeping and translates exceptions into FUSEError.

pyfuse3 keeps mount state in module-level globals (``pyfuse3.init`` /
``pyfuse3.main`` / ``pyfuse3.close``), so only one biofuse mount can be
active per process at a time. The ``Mount`` context manager enforces this
implicitly by being the only entry point.
"""

import errno
import logging
import os
import shutil
import stat
import subprocess
import threading

import pyfuse3
import trio

from biofuse import view as view_mod

logger = logging.getLogger(__name__)


class BiofuseOperations(pyfuse3.Operations):
    """A pyfuse3 Operations subclass over a FilesystemView."""

    enable_writeback_cache = False
    supports_dot_lookup = True

    def __init__(self, view: view_mod.FilesystemView) -> None:
        super().__init__()
        self._view = view
        self._uid = os.getuid()
        self._gid = os.getgid()
        self._lock = threading.Lock()

        entries = sorted(view.list(), key=lambda e: e.name)
        self._inode_to_name: dict[int, str] = {}
        self._name_to_inode: dict[str, int] = {}
        self._inode_to_entry: dict[int, view_mod.FileEntry] = {}
        for index, entry in enumerate(entries):
            inode = pyfuse3.ROOT_INODE + 1 + index
            self._inode_to_name[inode] = entry.name
            self._name_to_inode[entry.name] = inode
            self._inode_to_entry[inode] = entry

        self._next_fh = 1
        self._fh_to_view_handle: dict[int, int] = {}

    def _build_attrs(self, inode: int) -> pyfuse3.EntryAttributes:
        attrs = pyfuse3.EntryAttributes()
        attrs.st_ino = inode
        attrs.st_uid = self._uid
        attrs.st_gid = self._gid
        if inode == pyfuse3.ROOT_INODE:
            attrs.st_mode = stat.S_IFDIR | 0o555
            attrs.st_size = 0
            stamp = 0
        else:
            entry = self._inode_to_entry[inode]
            attrs.st_mode = entry.mode
            attrs.st_size = entry.size
            stamp = entry.mtime_ns
        attrs.st_atime_ns = stamp
        attrs.st_ctime_ns = stamp
        attrs.st_mtime_ns = stamp
        return attrs

    async def getattr(self, inode, ctx=None):
        if inode == pyfuse3.ROOT_INODE or inode in self._inode_to_entry:
            return self._build_attrs(inode)
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def lookup(self, parent_inode, name, ctx=None):
        if parent_inode != pyfuse3.ROOT_INODE:
            raise pyfuse3.FUSEError(errno.ENOENT)
        try:
            decoded = name.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise pyfuse3.FUSEError(errno.ENOENT) from exc
        inode = self._name_to_inode.get(decoded)
        if inode is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return self._build_attrs(inode)

    async def opendir(self, inode, ctx=None):
        if inode != pyfuse3.ROOT_INODE:
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        return inode

    async def readdir(self, fh, start_id, token):
        names = sorted(self._name_to_inode)
        for index, name in enumerate(names):
            entry_id = index + 1
            if entry_id <= start_id:
                continue
            inode = self._name_to_inode[name]
            attrs = self._build_attrs(inode)
            if not pyfuse3.readdir_reply(token, name.encode("utf-8"), attrs, entry_id):
                return

    async def releasedir(self, fh):
        return

    async def open(self, inode, flags, ctx=None):
        if flags & (os.O_WRONLY | os.O_RDWR) or flags & os.O_APPEND:
            raise pyfuse3.FUSEError(errno.EROFS)
        if inode not in self._inode_to_entry:
            raise pyfuse3.FUSEError(errno.ENOENT)
        name = self._inode_to_name[inode]
        try:
            view_fh = self._view.open(name)
        except FileNotFoundError as exc:
            raise pyfuse3.FUSEError(errno.ENOENT) from exc
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO) from exc
        with self._lock:
            fh = self._next_fh
            self._next_fh += 1
            self._fh_to_view_handle[fh] = view_fh
        return pyfuse3.FileInfo(fh=fh)

    async def read(self, fh, off, size):
        with self._lock:
            view_fh = self._fh_to_view_handle.get(fh)
        if view_fh is None:
            raise pyfuse3.FUSEError(errno.EBADF)
        try:
            return self._view.read(view_fh, off, size)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO) from exc

    async def release(self, fh):
        with self._lock:
            view_fh = self._fh_to_view_handle.pop(fh, None)
        if view_fh is None:
            return
        try:
            self._view.release(view_fh)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO) from exc

    async def forget(self, inode_list):
        return

    async def access(self, inode, mode, ctx=None):
        if mode & os.W_OK:
            raise pyfuse3.FUSEError(errno.EROFS)
        if inode == pyfuse3.ROOT_INODE or inode in self._inode_to_entry:
            return
        raise pyfuse3.FUSEError(errno.ENOENT)


class Mount:
    """Context manager that mounts a FilesystemView via pyfuse3.

    Mounts on ``__enter__``, runs the FUSE main loop on a background thread,
    and unmounts cleanly on ``__exit__``.

    pyfuse3's mount state is process-global, so only one Mount may be live
    in a given Python process at a time.
    """

    _global_lock = threading.Lock()
    _active: "Mount | None" = None

    def __init__(
        self,
        view: view_mod.FilesystemView,
        mountpoint: str,
        *,
        fsname: str = "biofuse",
        debug_fuse: bool = False,
    ) -> None:
        self._view = view
        self._mountpoint = mountpoint
        self._fsname = fsname
        self._debug_fuse = debug_fuse
        self._thread: threading.Thread | None = None
        self._exception: BaseException | None = None
        self._ops: BiofuseOperations | None = None
        self._closed = False

    def __enter__(self) -> str:
        if not Mount._global_lock.acquire(blocking=False):
            raise RuntimeError(
                "another biofuse Mount is active in this process; "
                "pyfuse3 supports only one mount at a time"
            )
        try:
            self._ops = BiofuseOperations(self._view)
            options = set(pyfuse3.default_options)
            options.add(f"fsname={self._fsname}")
            options.add("ro")
            if self._debug_fuse:
                options.add("debug")
            pyfuse3.init(self._ops, self._mountpoint, options)
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
