"""Streaming pyfuse3.Operations for a PLINK 1.9 view of a VCZ store.

Phase 2 strategy: rather than materialising the full ``.bed/.bim/.fam``
to disk, the encoder work happens in a separate worker subprocess
(see :mod:`biofuse.bed_worker` and :mod:`biofuse.bed_client`). This
module is the pyfuse3 adapter that owns inode/handle bookkeeping and
delegates every read to the client.

Design notes:

- The FUSE process imports neither ``vcztools`` nor ``zarr``. All Zarr
  metadata I/O and all ``BedEncoder`` state live in the worker.
- A FUSE handle (``fh``) maps 1:1 to a worker handle. ``open()`` asks
  the client for a fresh worker handle; ``release()`` releases it.
"""

import errno
import logging
import os
import stat
import threading
from typing import Protocol

import pyfuse3

from biofuse import access_log as access_log_mod
from biofuse import bed_protocol

logger = logging.getLogger(__name__)


class BedEncoderClientProto(Protocol):
    """The slice of :class:`biofuse.bed_client.BedEncoderClient` that
    :class:`PlinkOps` depends on.

    Defined as a Protocol so tests can inject a fake without spawning a
    subprocess.
    """

    @property
    def file_entries(self) -> list[bed_protocol.FileSpec]: ...
    async def open(self, name: str) -> tuple[int, int, int]: ...
    async def read(self, handle: int, offset: int, size: int) -> bytes: ...
    async def release(self, handle: int) -> None: ...


class PlinkOps(pyfuse3.Operations):
    """A pyfuse3 Operations subclass that serves a PLINK 1.9 view of a VCZ.

    Parameters
    ----------
    client
        A :class:`BedEncoderClient` (or any object satisfying
        :class:`BedEncoderClientProto`) connected to a worker. The
        client's ``file_entries`` define the visible files and their
        sizes; ``open``/``read``/``release`` are dispatched to it. The
        caller owns the client's lifetime.
    access_logger
        Optional ``AccessLogger`` to record per-FUSE-read traces.
    """

    enable_writeback_cache = False
    supports_dot_lookup = True

    def __init__(
        self,
        client: BedEncoderClientProto,
        *,
        access_logger: access_log_mod.AccessLogger | None = None,
    ) -> None:
        super().__init__()
        self._client = client
        self._access_logger = access_logger
        self._uid = os.getuid()
        self._gid = os.getgid()
        self._lock = threading.Lock()

        entries = sorted(client.file_entries, key=lambda spec: spec.name)
        self._inode_to_name: dict[int, str] = {}
        self._name_to_inode: dict[str, int] = {}
        self._inode_to_entry: dict[int, bed_protocol.FileSpec] = {}
        for index, entry in enumerate(entries):
            inode = pyfuse3.ROOT_INODE + 1 + index
            self._inode_to_name[inode] = entry.name
            self._name_to_inode[entry.name] = inode
            self._inode_to_entry[inode] = entry

        self._next_fh = 1
        self._fh_to_handle: dict[int, int] = {}
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
            entry = self._inode_to_entry[inode]
            attrs.st_mode = entry.mode
            attrs.st_size = entry.size
        attrs.st_atime_ns = 0
        attrs.st_ctime_ns = 0
        attrs.st_mtime_ns = 0
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
            handle, _size, _mode = await self._client.open(name)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO) from exc
        with self._lock:
            fh = self._next_fh
            self._next_fh += 1
            self._fh_to_handle[fh] = handle
            self._fh_to_name[fh] = name
        return pyfuse3.FileInfo(fh=fh)

    async def read(self, fh, off, size):
        with self._lock:
            handle = self._fh_to_handle.get(fh)
            name = self._fh_to_name.get(fh)
        if handle is None:
            raise pyfuse3.FUSEError(errno.EBADF)
        try:
            data = await self._client.read(handle, off, size)
        except OSError as exc:
            raise pyfuse3.FUSEError(exc.errno or errno.EIO) from exc
        if self._access_logger is not None:
            self._access_logger.record(name, off, len(data))
        return data

    async def release(self, fh):
        with self._lock:
            handle = self._fh_to_handle.pop(fh, None)
            self._fh_to_name.pop(fh, None)
        if handle is None:
            return
        try:
            await self._client.release(handle)
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
