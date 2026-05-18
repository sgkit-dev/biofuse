"""pyfuse3.Operations for an encoder-served view of a VCZ store.

Generic over the output format (PLINK 1.9 / Oxford BGEN). The active
:class:`~biofuse.formats.FormatSpec` defines:

- the streaming file suffix (``.bed`` / ``.bgen``) and its
  ``streaming_kind`` dispatch key;
- the static-sidecar suffixes (``.bim``/``.fam`` for PLINK,
  ``.sample``/``.bgen.bgi`` for BGEN), built once at mount time and
  held in the host's memory.

Static-sidecar reads are served directly from the cached bytes. Each
streaming-file ``open()`` from the kernel allocates a fresh
:class:`biofuse.encoder_host.StreamHandle` — a dedicated
:class:`~vcztools.format_encoder.FormatEncoder` whose blocking work
runs on worker threads via :func:`trio.to_thread.run_sync`, leaving
the pyfuse3 trio task free for other FUSE requests. ``read()`` on
that fh dispatches one ``encoder.read`` call per request; the
kernel's parallel readahead requests on a single fh serialise on the
``StreamHandle``'s internal lock. ``release()`` closes the encoder.
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
from biofuse import formats

logger = logging.getLogger(__name__)


_FILE_MODE = stat.S_IFREG | 0o444
_DEFAULT_MAX_OPEN_STREAM = 16

# Maximum time a FUSE_OPEN may wait at the per-mount streaming-file
# capacity limiter. On expiry the open returns ``EAGAIN`` to the kernel
# rather than blocking forever — this guards against a leaked limiter
# slot permanently wedging open(). The timeout is also recorded in the
# access log as a ``limiter_timeout`` event so a post-hoc trace can
# tell EAGAIN-from-limiter from any other EAGAIN.
_LIMITER_TIMEOUT_S = 30.0

_STATIC_KIND = "static"


class _StreamHandleProto(Protocol):
    async def read(self, off: int, size: int) -> bytes: ...
    async def aclose(self) -> None: ...


class EncoderHostProto(Protocol):
    """The slice of :class:`biofuse.encoder_host.EncoderHost` that
    :class:`EncoderOps` depends on.

    Defined as a Protocol so tests can inject a fake without opening
    a real reader.
    """

    static_files: dict[str, bytes]
    stream_size: int

    async def open_stream(self) -> _StreamHandleProto: ...


class EncoderOps(pyfuse3.Operations):
    """A pyfuse3 Operations subclass that serves an encoder-rendered view of a VCZ.

    Parameters
    ----------
    host
        An :class:`~biofuse.encoder_host.EncoderHost` whose ``start``
        has populated ``static_files`` and ``stream_size`` (or any
        object satisfying :class:`EncoderHostProto`). The caller owns
        the host's lifetime.
    basename
        Stem used for the exposed files: ``{basename}{spec.streaming_suffix}``
        plus one ``{basename}{suffix}`` per entry of ``host.static_files``.
    spec
        The active :class:`~biofuse.formats.FormatSpec`. Its
        ``streaming_suffix`` is exposed as the streaming filename;
        ``streaming_kind`` is the dispatch key for read routing.
        The set of static suffixes is read off ``host.static_files``,
        which is the post-options filtered set the host actually
        produced.
    max_open_stream
        Maximum number of concurrent open streaming-file fhs. New
        opens block until a peer release frees a slot. Static-file
        opens are unaffected — they're served from cached bytes and
        carry no per-fh state worth queueing.
    access_logger
        Optional ``AccessLogger`` to record per-FUSE-read traces.
    """

    enable_writeback_cache = False
    supports_dot_lookup = True

    def __init__(
        self,
        host: EncoderHostProto,
        basename: str,
        spec: formats.FormatSpec,
        *,
        max_open_stream: int = _DEFAULT_MAX_OPEN_STREAM,
        access_logger: access_log_mod.AccessLogger | None = None,
    ) -> None:
        super().__init__()
        self._host = host
        self._basename = basename
        self._spec = spec
        self._access_logger = access_logger
        self._uid = os.getuid()
        self._gid = os.getgid()
        self._stream_limiter = trio.CapacityLimiter(max_open_stream)

        # Build the file manifest: one streaming file plus one entry per
        # static suffix. Each entry has a kind that drives read dispatch:
        # the streaming file's kind is the spec's ``streaming_kind``;
        # all static files share the same ``_STATIC_KIND`` dispatch key.
        # ``_name_to_suffix`` maps each static filename to its suffix
        # for lookups into ``host.static_files``.
        stream_name = f"{basename}{spec.streaming_suffix}"
        manifest: list[tuple[str, str, int]] = [
            (stream_name, spec.streaming_kind, host.stream_size)
        ]
        self._name_to_suffix: dict[str, str] = {}
        for suffix, body in host.static_files.items():
            name = f"{basename}{suffix}"
            manifest.append((name, _STATIC_KIND, len(body)))
            self._name_to_suffix[name] = suffix
        manifest.sort()

        self._inode_to_name: dict[int, str] = {}
        self._name_to_inode: dict[str, int] = {}
        self._inode_to_size: dict[int, int] = {}
        self._name_to_kind: dict[str, str] = {}
        for index, (name, kind, size) in enumerate(manifest):
            inode = pyfuse3.ROOT_INODE + 1 + index
            self._inode_to_name[inode] = name
            self._name_to_inode[name] = inode
            self._inode_to_size[inode] = size
            self._name_to_kind[name] = kind

        self._next_fh = 1
        self._fh_to_kind: dict[int, str] = {}
        self._fh_to_name: dict[int, str] = {}
        self._fh_to_conn: dict[int, _StreamHandleProto] = {}

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
        t_open_start = time.monotonic()
        if kind == self._spec.streaming_kind:
            # Use the fh itself as the limiter borrower: each open is a
            # distinct logical owner, even when several share the same
            # trio task (true under direct EncoderOps tests, and cheap
            # under pyfuse3 where each request is its own task).
            t_limiter_start = time.monotonic()
            with trio.move_on_after(_LIMITER_TIMEOUT_S) as cs:
                await self._stream_limiter.acquire_on_behalf_of(fh)
            t_limiter_end = time.monotonic()
            if cs.cancelled_caught:
                self._record_event(
                    "limiter_timeout", name, fh, t_limiter_start, t_limiter_end
                )
                raise pyfuse3.FUSEError(errno.EAGAIN)
            self._record_event("limiter_wait", name, fh, t_limiter_start, t_limiter_end)
            try:
                on_aclose = self._make_aclose_recorder(name, fh)
                conn = await self._host.open_stream(on_aclose=on_aclose)
            except OSError as exc:
                self._stream_limiter.release_on_behalf_of(fh)
                raise pyfuse3.FUSEError(exc.errno or errno.EIO) from exc
            except BaseException:
                self._stream_limiter.release_on_behalf_of(fh)
                raise
            self._fh_to_conn[fh] = conn
        self._fh_to_kind[fh] = kind
        self._fh_to_name[fh] = name
        self._record_event("open", name, fh, t_open_start)
        return pyfuse3.FileInfo(fh=fh)

    def _record_event(
        self, kind: str, name: str, fh: int, t_start: float, t_end=None
    ) -> None:
        if self._access_logger is not None:
            self._access_logger.record_event(kind, name, fh, t_start, t_end)

    def _make_aclose_recorder(self, name, fh):
        if self._access_logger is None:
            return None
        access_logger = self._access_logger

        def hook(t_start: float, t_end: float) -> None:
            access_logger.record_event("aclose", name, fh, t_start, t_end)

        return hook

    async def read(self, fh, off, size):
        kind = self._fh_to_kind.get(fh)
        name = self._fh_to_name.get(fh)
        conn = self._fh_to_conn.get(fh)
        if kind is None or name is None:
            raise pyfuse3.FUSEError(errno.EBADF)
        t_start = time.monotonic()
        if kind == _STATIC_KIND:
            suffix = self._name_to_suffix[name]
            data = self._read_static(self._host.static_files[suffix], off, size)
        elif kind == self._spec.streaming_kind:
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
        name = self._fh_to_name.pop(fh, None)
        conn = self._fh_to_conn.pop(fh, None)
        if kind is None:
            return
        t_release_start = time.monotonic()
        try:
            if conn is not None:
                try:
                    await conn.aclose()
                except Exception as exc:  # noqa: BLE001 - best-effort cleanup
                    logger.debug("stream connection close raised: %s", exc)
        finally:
            if kind == self._spec.streaming_kind:
                self._stream_limiter.release_on_behalf_of(fh)
            self._record_event("release", name, fh, t_release_start)

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
