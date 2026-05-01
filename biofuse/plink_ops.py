"""Streaming pyfuse3.Operations for a PLINK 1.9 view of a VCZ store.

Phase 2 strategy: rather than materialising the full ``.bed/.bim/.fam`` to
disk and serving it through a passthrough view, this module wraps the
``vcztools.BedEncoder`` directly. Each FUSE ``open()`` of ``.bed`` allocates
a fresh encoder bound to a long-lived chunk iterator; ``.bim`` and ``.fam``
are precomputed once and served from memory.

Design notes:

- Per-handle ``BedEncoder`` keeps each consumer's iterator state isolated.
  Multiple plink processes mounting the same path each get a distinct kernel
  ``fh`` and therefore a distinct encoder; the underlying ``VczReader`` is
  shared (``vcztools.plink.BedEncoder`` documents that as safe).
- ``.bim`` and ``.fam`` are computed eagerly via ``generate_bim`` /
  ``generate_fam`` at construction time so the first read does not block the
  FUSE main loop on Zarr I/O.
- The flat-directory bookkeeping (inode allocation, ``getattr``/``lookup``/
  ``readdir``/``access``) mirrors ``BiofuseOperations`` — different per-handle
  backend, same read-only top-level directory shape.
"""

import errno
import logging
import os
import stat
import threading

import pyfuse3
from vcztools import plink as vcztools_plink
from vcztools import retrieval as vcztools_retrieval

from biofuse import access_log as access_log_mod
from biofuse import view as view_mod

logger = logging.getLogger(__name__)


class _StaticBytesFile:
    """Per-handle adapter for a file whose bytes are precomputed in memory.

    Implements the same ``read(off, size) -> bytes`` and ``close()`` shape as
    ``vcztools.plink.BedEncoder`` so ``PlinkOps`` can dispatch uniformly.
    """

    def __init__(self, name: str, data: bytes) -> None:
        self._name = name
        self._data = data
        self._closed = False

    @property
    def name(self) -> str:
        return self._name

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


class PlinkOps(pyfuse3.Operations):
    """A pyfuse3 Operations subclass that serves a PLINK 1.9 view of a VCZ.

    Parameters
    ----------
    reader
        Already-constructed ``VczReader`` over the source store. The reader's
        lifetime is owned by the caller; ``PlinkOps`` does not close it.
        Must not have had ``set_variants()`` or any subset call applied
        (``BedEncoder`` requires the default full-store variant axis).
    basename
        Stem used for the three exposed files: ``{basename}.bed``,
        ``{basename}.bim``, ``{basename}.fam``.
    access_logger
        Optional ``AccessLogger`` to record per-FUSE-read traces. Useful for
        comparing streaming-mode access patterns against phase-1 baselines.
    """

    enable_writeback_cache = False
    supports_dot_lookup = True

    def __init__(
        self,
        reader: vcztools_retrieval.VczReader,
        basename: str,
        *,
        access_logger: access_log_mod.AccessLogger | None = None,
    ) -> None:
        super().__init__()
        self._reader = reader
        self._access_logger = access_logger
        self._uid = os.getuid()
        self._gid = os.getgid()
        self._lock = threading.Lock()

        bim_text = vcztools_plink.generate_bim(reader)
        fam_text = vcztools_plink.generate_fam(reader)
        self._bim_bytes = bim_text.encode("utf-8")
        self._fam_bytes = fam_text.encode("utf-8")

        num_variants = reader.num_variants
        num_samples = int(reader.sample_ids.size)
        bytes_per_variant = (num_samples + 3) // 4
        bed_size = 3 + num_variants * bytes_per_variant

        bed_name = f"{basename}.bed"
        bim_name = f"{basename}.bim"
        fam_name = f"{basename}.fam"
        entries = sorted(
            [
                view_mod.FileEntry(
                    name=bed_name,
                    size=bed_size,
                    mtime_ns=0,
                    mode=stat.S_IFREG | 0o444,
                ),
                view_mod.FileEntry(
                    name=bim_name,
                    size=len(self._bim_bytes),
                    mtime_ns=0,
                    mode=stat.S_IFREG | 0o444,
                ),
                view_mod.FileEntry(
                    name=fam_name,
                    size=len(self._fam_bytes),
                    mtime_ns=0,
                    mode=stat.S_IFREG | 0o444,
                ),
            ],
            key=lambda e: e.name,
        )
        self._bed_name = bed_name
        self._bim_name = bim_name
        self._fam_name = fam_name

        self._inode_to_name: dict[int, str] = {}
        self._name_to_inode: dict[str, int] = {}
        self._inode_to_entry: dict[int, view_mod.FileEntry] = {}
        for index, entry in enumerate(entries):
            inode = pyfuse3.ROOT_INODE + 1 + index
            self._inode_to_name[inode] = entry.name
            self._name_to_inode[entry.name] = inode
            self._inode_to_entry[inode] = entry

        self._next_fh = 1
        self._open_files: dict[
            int, _StaticBytesFile | vcztools_plink.BedEncoder
        ] = {}

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
        if name == self._bed_name:
            backend = vcztools_plink.BedEncoder(self._reader)
        elif name == self._bim_name:
            backend = _StaticBytesFile(name, self._bim_bytes)
        elif name == self._fam_name:
            backend = _StaticBytesFile(name, self._fam_bytes)
        else:
            raise pyfuse3.FUSEError(errno.ENOENT)
        with self._lock:
            fh = self._next_fh
            self._next_fh += 1
            self._open_files[fh] = backend
        return pyfuse3.FileInfo(fh=fh)

    async def read(self, fh, off, size):
        with self._lock:
            backend = self._open_files.get(fh)
        if backend is None:
            raise pyfuse3.FUSEError(errno.EBADF)
        try:
            data = backend.read(off, size)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO) from exc
        if self._access_logger is not None:
            name = backend.name if isinstance(backend, _StaticBytesFile) else self._bed_name
            self._access_logger.record(name, off, len(data))
        return data

    async def release(self, fh):
        with self._lock:
            backend = self._open_files.pop(fh, None)
        if backend is None:
            return
        backend.close()

    async def forget(self, inode_list):
        return

    async def access(self, inode, mode, ctx=None):
        if mode & os.W_OK:
            raise pyfuse3.FUSEError(errno.EROFS)
        if inode == pyfuse3.ROOT_INODE or inode in self._inode_to_entry:
            return
        raise pyfuse3.FUSEError(errno.ENOENT)
