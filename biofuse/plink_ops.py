"""pyfuse3.Operations for a PLINK 1.9 view of a VCZ store.

The static ``.bim`` and ``.fam`` bytes are fetched once at mount time
and held in memory by the FUSE adapter; reads against them are served
directly from the cached bytes without crossing process boundaries.

Each ``.bed`` ``open()`` from the kernel allocates a fresh
:class:`biofuse.plink_client.BedConnection` — a dedicated socket to
the plink-server subprocess, where one server thread with one
``BedEncoder`` runs synchronously for the lifetime of the connection.
``read()`` on that fh forwards straight to the per-fh socket; the
kernel's parallel readahead requests on a single fh serialise on the
``BedConnection``'s internal lock. ``release()`` closes the socket;
the server thread sees EOF and exits.
"""

import errno
import logging
import os
import stat
import time
from typing import Protocol

import pyfuse3
import trio

from biofuse import access_log as access_log_mod

logger = logging.getLogger(__name__)


_FILE_MODE = stat.S_IFREG | 0o444
_DEFAULT_MAX_OPEN_BED = 16


class _BedConnectionProto(Protocol):
    async def read(self, off: int, size: int) -> bytes: ...
    async def aclose(self) -> None: ...


class PlinkClientProto(Protocol):
    """The slice of :class:`biofuse.plink_client.PlinkClient` that
    :class:`PlinkOps` depends on.

    Defined as a Protocol so tests can inject a fake without spawning
    a subprocess.
    """

    bim_bytes: bytes
    fam_bytes: bytes
    bed_size: int

    async def open_bed(self) -> _BedConnectionProto: ...


class PlinkOps(pyfuse3.Operations):
    """A pyfuse3 Operations subclass that serves a PLINK 1.9 view of a VCZ.

    Parameters
    ----------
    client
        A :class:`PlinkClient` already past its metadata handshake (or
        any object satisfying :class:`PlinkClientProto`). The caller
        owns the client's lifetime.
    basename
        Stem used for the three exposed files: ``{basename}.bed``,
        ``{basename}.bim``, ``{basename}.fam``.
    max_open_bed
        Maximum number of concurrent open ``.bed`` files. New ``.bed``
        opens block until a peer release frees a slot. ``.bim``/``.fam``
        opens are unaffected — they're served from cached static bytes
        and carry no per-fh state worth queueing.
    access_logger
        Optional ``AccessLogger`` to record per-FUSE-read traces.
    """

    enable_writeback_cache = False
    supports_dot_lookup = True

    def __init__(
        self,
        client: PlinkClientProto,
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
        self._bed_limiter = trio.CapacityLimiter(max_open_bed)

        bed_name = f"{basename}.bed"
        bim_name = f"{basename}.bim"
        fam_name = f"{basename}.fam"
        files = sorted(
            [
                (bed_name, "bed", client.bed_size),
                (bim_name, "bim", len(client.bim_bytes)),
                (fam_name, "fam", len(client.fam_bytes)),
            ]
        )
        self._inode_to_name: dict[int, str] = {}
        self._name_to_inode: dict[str, int] = {}
        self._inode_to_size: dict[int, int] = {}
        self._name_to_kind: dict[str, str] = {}
        for index, (name, kind, size) in enumerate(files):
            inode = pyfuse3.ROOT_INODE + 1 + index
            self._inode_to_name[inode] = name
            self._name_to_inode[name] = inode
            self._inode_to_size[inode] = size
            self._name_to_kind[name] = kind

        self._next_fh = 1
        self._fh_to_kind: dict[int, str] = {}
        self._fh_to_name: dict[int, str] = {}
        self._fh_to_conn: dict[int, _BedConnectionProto] = {}

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
        kind = self._name_to_kind[name]
        fh = self._next_fh
        self._next_fh += 1
        if kind == "bed":
            # Use the fh itself as the limiter borrower: each open is a
            # distinct logical owner, even when several share the same
            # trio task (true under direct PlinkOps tests, and cheap
            # under pyfuse3 where each request is its own task).
            await self._bed_limiter.acquire_on_behalf_of(fh)
            try:
                conn = await self._client.open_bed()
            except OSError as exc:
                self._bed_limiter.release_on_behalf_of(fh)
                raise pyfuse3.FUSEError(exc.errno or errno.EIO) from exc
            except BaseException:
                self._bed_limiter.release_on_behalf_of(fh)
                raise
            self._fh_to_conn[fh] = conn
        self._fh_to_kind[fh] = kind
        self._fh_to_name[fh] = name
        return pyfuse3.FileInfo(fh=fh)

    async def read(self, fh, off, size):
        kind = self._fh_to_kind.get(fh)
        name = self._fh_to_name.get(fh)
        conn = self._fh_to_conn.get(fh)
        if kind is None:
            raise pyfuse3.FUSEError(errno.EBADF)
        t_start = time.monotonic()
        if kind == "bim":
            data = self._read_static(self._client.bim_bytes, off, size)
        elif kind == "fam":
            data = self._read_static(self._client.fam_bytes, off, size)
        elif kind == "bed":
            assert conn is not None
            try:
                data = await conn.read(off, size)
            except OSError as exc:
                raise pyfuse3.FUSEError(exc.errno or errno.EIO) from exc
        else:
            raise pyfuse3.FUSEError(errno.EBADF)
        if self._access_logger is not None:
            self._access_logger.record(name, fh, off, len(data), t_start)
        return data

    @staticmethod
    def _read_static(data: bytes, off: int, size: int) -> bytes:
        if off >= len(data) or size == 0:
            return b""
        end = min(off + size, len(data))
        return bytes(data[off:end])

    async def release(self, fh):
        kind = self._fh_to_kind.pop(fh, None)
        self._fh_to_name.pop(fh, None)
        conn = self._fh_to_conn.pop(fh, None)
        if kind is None:
            return
        try:
            if conn is not None:
                try:
                    await conn.aclose()
                except Exception as exc:  # noqa: BLE001 - best-effort cleanup
                    logger.debug("bed connection close raised: %s", exc)
        finally:
            if kind == "bed":
                self._bed_limiter.release_on_behalf_of(fh)

    async def forget(self, inode_list):
        return

    async def access(self, inode, mode, ctx=None):
        if mode & os.W_OK:
            raise pyfuse3.FUSEError(errno.EROFS)
        if inode == pyfuse3.ROOT_INODE or inode in self._inode_to_size:
            return
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def statfs(self, ctx=None):
        block_size = 4096
        total_bytes = sum(self._inode_to_size.values())
        out = pyfuse3.StatvfsData()
        out.f_bsize = block_size
        out.f_frsize = block_size
        out.f_blocks = (total_bytes + block_size - 1) // block_size
        out.f_bfree = 0
        out.f_bavail = 0
        out.f_files = len(self._inode_to_size)
        out.f_ffree = 0
        out.f_favail = 0
        out.f_namemax = 255
        return out
