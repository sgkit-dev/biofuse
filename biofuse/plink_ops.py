"""Streaming pyfuse3.Operations for a PLINK 1.9 view of a VCZ store.

Phase 2 strategy: rather than materialising the full ``.bed/.bim/.fam``
to disk, the encoder work happens in a separate worker subprocess
(see :mod:`biofuse.bed_worker` and :mod:`biofuse.bed_client`). This
module is the pyfuse3 adapter that owns inode/handle bookkeeping,
maps filenames to :class:`bed_protocol.FileType` tags, and delegates
every read to the client.

Design notes:

- The FUSE process imports neither ``vcztools`` nor ``zarr``. All Zarr
  metadata I/O and all ``BedEncoder`` state live in the worker.
- PlinkOps owns all filename logic. Filenames never cross the wire to
  the worker; the worker only sees opaque ``fh`` integers tagged with
  a ``FileType``.
- A FUSE handle (``fh``) maps 1:1 to a worker handle. PlinkOps
  allocates the fh, asks the client to open it as a given
  ``FileType``, releases it on FUSE release.
- A :class:`trio.CapacityLimiter` caps concurrent open ``.bed`` files.
  ``.bim`` and ``.fam`` are precomputed static bytes — cheap, never
  blocked. Only ``BedEncoder`` keeps Zarr iterator state alive, so
  the cap meaningfully bounds peak resource use under parallel
  consumers without delaying trivial reads of the metadata files.
"""

import errno
import logging
import os
import stat
import threading
from typing import Protocol

import pyfuse3
import trio

from biofuse import access_log as access_log_mod
from biofuse import bed_protocol

logger = logging.getLogger(__name__)


_DEFAULT_MAX_OPEN_BED = 16
_FILE_MODE = stat.S_IFREG | 0o444


class BedEncoderClientProto(Protocol):
    """The slice of :class:`biofuse.bed_client.BedEncoderClient` that
    :class:`PlinkOps` depends on.

    Defined as a Protocol so tests can inject a fake without spawning
    a subprocess.
    """

    @property
    def file_entries(self) -> dict[bed_protocol.FileType, int]: ...
    async def open(self, fh: int, file_type: bed_protocol.FileType) -> None: ...
    async def read(self, fh: int, offset: int, size: int) -> bytes: ...
    async def release(self, fh: int) -> None: ...


class PlinkOps(pyfuse3.Operations):
    """A pyfuse3 Operations subclass that serves a PLINK 1.9 view of a VCZ.

    Parameters
    ----------
    client
        A :class:`BedEncoderClient` (or any object satisfying
        :class:`BedEncoderClientProto`) connected to a worker. The
        caller owns the client's lifetime.
    basename
        Stem used for the three exposed files: ``{basename}.bed``,
        ``{basename}.bim``, ``{basename}.fam``.
    max_open_bed
        Maximum number of concurrent open ``.bed`` files. New ``.bed``
        opens block until a peer release frees a slot. ``.bim``/``.fam``
        opens are unaffected.
    access_logger
        Optional ``AccessLogger`` to record per-FUSE-read traces.
    """

    enable_writeback_cache = False
    supports_dot_lookup = True

    def __init__(
        self,
        client: BedEncoderClientProto,
        basename: str,
        *,
        max_open_bed: int = _DEFAULT_MAX_OPEN_BED,
        access_logger: access_log_mod.AccessLogger | None = None,
    ) -> None:
        super().__init__()
        self._client = client
        self._basename = basename
        self._access_logger = access_logger
        self._uid = os.getuid()
        self._gid = os.getgid()
        self._lock = threading.Lock()
        self._bed_limiter = trio.CapacityLimiter(max_open_bed)

        sizes = client.file_entries
        bed_name = f"{basename}.bed"
        bim_name = f"{basename}.bim"
        fam_name = f"{basename}.fam"
        files = sorted(
            [
                (bed_name, bed_protocol.FileType.BED, sizes[bed_protocol.FileType.BED]),
                (bim_name, bed_protocol.FileType.BIM, sizes[bed_protocol.FileType.BIM]),
                (fam_name, bed_protocol.FileType.FAM, sizes[bed_protocol.FileType.FAM]),
            ]
        )
        self._inode_to_name: dict[int, str] = {}
        self._name_to_inode: dict[str, int] = {}
        self._inode_to_size: dict[int, int] = {}
        self._name_to_type: dict[str, bed_protocol.FileType] = {}
        for index, (name, file_type, size) in enumerate(files):
            inode = pyfuse3.ROOT_INODE + 1 + index
            self._inode_to_name[inode] = name
            self._name_to_inode[name] = inode
            self._inode_to_size[inode] = size
            self._name_to_type[name] = file_type

        self._next_fh = 1
        self._fh_to_type: dict[int, bed_protocol.FileType] = {}
        self._fh_to_name: dict[int, str] = {}

    def _build_attrs(self, inode: int) -> pyfuse3.EntryAttributes:
        attrs = pyfuse3.EntryAttributes()
        attrs.st_ino = inode
        attrs.st_uid = self._uid
        attrs.st_gid = self._gid
        if inode == pyfuse3.ROOT_INODE:
            attrs.st_mode = stat.S_IFDIR | 0o555
            attrs.st_size = 0
        else:
            attrs.st_mode = _FILE_MODE
            attrs.st_size = self._inode_to_size[inode]
        attrs.st_atime_ns = 0
        attrs.st_ctime_ns = 0
        attrs.st_mtime_ns = 0
        return attrs

    async def getattr(self, inode, ctx=None):
        if inode == pyfuse3.ROOT_INODE or inode in self._inode_to_size:
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
        if inode not in self._inode_to_size:
            raise pyfuse3.FUSEError(errno.ENOENT)
        name = self._inode_to_name[inode]
        file_type = self._name_to_type[name]
        with self._lock:
            fh = self._next_fh
            self._next_fh += 1
        if file_type is bed_protocol.FileType.BED:
            # Use the fh itself as the limiter borrower: each open is a
            # distinct logical owner, even when several share the same
            # trio task (true under direct PlinkOps tests, and cheap
            # under pyfuse3 where each request is its own task).
            await self._bed_limiter.acquire_on_behalf_of(fh)
        try:
            await self._client.open(fh, file_type)
        except OSError as exc:
            if file_type is bed_protocol.FileType.BED:
                self._bed_limiter.release_on_behalf_of(fh)
            raise pyfuse3.FUSEError(exc.errno or errno.EIO) from exc
        except BaseException:
            if file_type is bed_protocol.FileType.BED:
                self._bed_limiter.release_on_behalf_of(fh)
            raise
        # No more awaits below — sync dict updates, no checkpoint, so
        # cancellation can't strand the limiter slot.
        with self._lock:
            self._fh_to_type[fh] = file_type
            self._fh_to_name[fh] = name
        return pyfuse3.FileInfo(fh=fh)

    async def read(self, fh, off, size):
        with self._lock:
            file_type = self._fh_to_type.get(fh)
            name = self._fh_to_name.get(fh)
        if file_type is None:
            raise pyfuse3.FUSEError(errno.EBADF)
        try:
            data = await self._client.read(fh, off, size)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO) from exc
        if self._access_logger is not None:
            self._access_logger.record(name, off, len(data))
        return data

    async def release(self, fh):
        with self._lock:
            file_type = self._fh_to_type.pop(fh, None)
            self._fh_to_name.pop(fh, None)
        if file_type is None:
            return
        try:
            await self._client.release(fh)
        finally:
            if file_type is bed_protocol.FileType.BED:
                self._bed_limiter.release_on_behalf_of(fh)

    async def forget(self, inode_list):
        return

    async def access(self, inode, mode, ctx=None):
        if mode & os.W_OK:
            raise pyfuse3.FUSEError(errno.EROFS)
        if inode == pyfuse3.ROOT_INODE or inode in self._inode_to_size:
            return
        raise pyfuse3.FUSEError(errno.ENOENT)
