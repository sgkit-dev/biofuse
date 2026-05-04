"""Unit tests for PlinkOps.

Exercises the streaming Operations class via direct async-method calls
— no kernel mount, no subprocess. The vcztools/Zarr-side parity tests
live in ``test_bed_worker.py`` (against ``WorkerSession`` directly) and
in ``test_bed_client.py`` (against a real subprocess). End-to-end FUSE
behaviour against real plink binaries is in ``test_plink_apps.py``.
"""

import errno
import os
import stat

import pyfuse3
import pytest
import trio

from biofuse import access_log, bed_protocol, plink_ops


def run(coro):
    return trio.run(lambda: coro)


class _FakeClient:
    """In-process stand-in for :class:`biofuse.bed_client.BedEncoderClient`.

    Records the call sequence so tests can assert PlinkOps dispatched
    correctly. Every :meth:`open` returns a fresh handle id; reads return
    deterministic bytes derived from ``(handle, offset, size)`` for parity
    checks. Errors can be queued via :meth:`raise_on_next`.
    """

    def __init__(self, file_entries: list[bed_protocol.FileSpec]) -> None:
        self._entries = list(file_entries)
        self._next_handle = 100
        self._open_handles: dict[int, str] = {}
        self.calls: list[tuple] = []
        self._next_error: tuple[str, OSError] | None = None

    @property
    def file_entries(self) -> list[bed_protocol.FileSpec]:
        return list(self._entries)

    def raise_on_next(self, op: str, exc: OSError) -> None:
        self._next_error = (op, exc)

    def _maybe_raise(self, op: str) -> None:
        if self._next_error is not None and self._next_error[0] == op:
            _, exc = self._next_error
            self._next_error = None
            raise exc

    async def open(self, name: str) -> tuple[int, int, int]:
        self.calls.append(("open", name))
        self._maybe_raise("open")
        spec = next((s for s in self._entries if s.name == name), None)
        if spec is None:
            raise OSError(errno.ENOENT, name)
        handle = self._next_handle
        self._next_handle += 1
        self._open_handles[handle] = name
        return handle, spec.size, spec.mode

    async def read(self, handle: int, offset: int, size: int) -> bytes:
        self.calls.append(("read", handle, offset, size))
        self._maybe_raise("read")
        if handle not in self._open_handles:
            raise OSError(errno.EBADF, "unknown handle")
        # Deterministic, easy-to-recompute bytes.
        return bytes(((offset + i) & 0xFF) for i in range(size))

    async def release(self, handle: int) -> None:
        self.calls.append(("release", handle))
        self._maybe_raise("release")
        self._open_handles.pop(handle, None)


def _default_entries() -> list[bed_protocol.FileSpec]:
    mode = stat.S_IFREG | 0o444
    return [
        bed_protocol.FileSpec("small.bed", 1024, mode),
        bed_protocol.FileSpec("small.bim", 256, mode),
        bed_protocol.FileSpec("small.fam", 100, mode),
    ]


@pytest.fixture
def fx_client():
    return _FakeClient(_default_entries())


@pytest.fixture
def fx_ops(fx_client):
    return plink_ops.PlinkOps(fx_client)


def _expect_fuse_error(coro, expected_errno):
    with pytest.raises(pyfuse3.FUSEError) as excinfo:
        run(coro)
    assert excinfo.value.errno == expected_errno


class TestConstructor:
    def test_creates_three_inodes(self, fx_ops):
        assert len(fx_ops._inode_to_entry) == 3
        names = sorted(fx_ops._name_to_inode)
        assert names == ["small.bed", "small.bim", "small.fam"]

    def test_basenames_propagate_from_client(self):
        mode = stat.S_IFREG | 0o444
        client = _FakeClient(
            [
                bed_protocol.FileSpec("alt.bed", 10, mode),
                bed_protocol.FileSpec("alt.bim", 5, mode),
                bed_protocol.FileSpec("alt.fam", 2, mode),
            ]
        )
        ops = plink_ops.PlinkOps(client)
        assert sorted(ops._name_to_inode) == ["alt.bed", "alt.bim", "alt.fam"]

    def test_sizes_match_client_entries(self, fx_ops):
        sizes = {
            fx_ops._inode_to_name[i]: e.size
            for i, e in fx_ops._inode_to_entry.items()
        }
        assert sizes == {"small.bed": 1024, "small.bim": 256, "small.fam": 100}

    def test_inodes_assigned_in_sorted_order(self, fx_ops):
        names_in_order = [
            fx_ops._inode_to_name[i] for i in sorted(fx_ops._inode_to_name)
        ]
        assert names_in_order == sorted(names_in_order)


class TestGetattr:
    def test_root(self, fx_ops):
        attrs = run(fx_ops.getattr(pyfuse3.ROOT_INODE))
        assert stat.S_ISDIR(attrs.st_mode)

    def test_regular_file(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        attrs = run(fx_ops.getattr(inode))
        assert stat.S_ISREG(attrs.st_mode)
        assert attrs.st_size == 1024

    def test_unknown_inode(self, fx_ops):
        _expect_fuse_error(fx_ops.getattr(9999), errno.ENOENT)


class TestLookup:
    def test_known_name(self, fx_ops):
        attrs = run(fx_ops.lookup(pyfuse3.ROOT_INODE, b"small.bed"))
        assert attrs.st_size == 1024

    def test_unknown_name(self, fx_ops):
        _expect_fuse_error(fx_ops.lookup(pyfuse3.ROOT_INODE, b"nope.bed"), errno.ENOENT)

    def test_invalid_utf8(self, fx_ops):
        _expect_fuse_error(
            fx_ops.lookup(pyfuse3.ROOT_INODE, b"\xff\xfe.bed"), errno.ENOENT
        )

    def test_lookup_in_non_root(self, fx_ops):
        _expect_fuse_error(fx_ops.lookup(2, b"small.bed"), errno.ENOENT)


class TestReaddir:
    def test_yields_three_entries_in_sorted_order(self, fx_ops):
        emitted: list[tuple[str, int]] = []

        class FakeToken:
            pass

        token = FakeToken()

        def fake_readdir_reply(tok, name, attrs, next_id):
            emitted.append((name.decode("utf-8"), next_id))
            return True

        original = pyfuse3.readdir_reply
        pyfuse3.readdir_reply = fake_readdir_reply
        try:
            run(fx_ops.readdir(pyfuse3.ROOT_INODE, 0, token))
        finally:
            pyfuse3.readdir_reply = original
        assert [n for n, _ in emitted] == ["small.bed", "small.bim", "small.fam"]

    def test_readdir_resumes_from_start_id(self, fx_ops):
        emitted: list[str] = []

        def fake_readdir_reply(tok, name, attrs, next_id):
            emitted.append(name.decode("utf-8"))
            return True

        original = pyfuse3.readdir_reply
        pyfuse3.readdir_reply = fake_readdir_reply
        try:
            run(fx_ops.readdir(pyfuse3.ROOT_INODE, 1, object()))
        finally:
            pyfuse3.readdir_reply = original
        assert emitted == ["small.bim", "small.fam"]


class TestOpenFlags:
    def test_open_with_write_flag_returns_erofs(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        _expect_fuse_error(fx_ops.open(inode, os.O_WRONLY), errno.EROFS)

    def test_open_with_rdwr_flag_returns_erofs(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        _expect_fuse_error(fx_ops.open(inode, os.O_RDWR), errno.EROFS)

    def test_open_with_append_flag_returns_erofs(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        _expect_fuse_error(fx_ops.open(inode, os.O_RDONLY | os.O_APPEND), errno.EROFS)

    def test_open_unknown_inode(self, fx_ops):
        _expect_fuse_error(fx_ops.open(9999, os.O_RDONLY), errno.ENOENT)


class TestOpenDispatch:
    def test_open_calls_client_with_filename(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            assert ("open", "small.bed") in fx_client.calls
            assert info.fh in fx_ops._fh_to_handle
        finally:
            run(fx_ops.release(info.fh))

    def test_each_open_gets_distinct_fh(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        info1 = run(fx_ops.open(inode, os.O_RDONLY))
        info2 = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            assert info1.fh != info2.fh
        finally:
            run(fx_ops.release(info1.fh))
            run(fx_ops.release(info2.fh))

    def test_open_propagates_oserror_as_fuseerror(self, fx_ops, fx_client):
        fx_client.raise_on_next("open", OSError(errno.EACCES, "denied"))
        inode = fx_ops._name_to_inode["small.bed"]
        _expect_fuse_error(fx_ops.open(inode, os.O_RDONLY), errno.EACCES)


class TestRead:
    def test_read_dispatches_to_client(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            data = run(fx_ops.read(info.fh, 16, 8))
            handle = fx_ops._fh_to_handle[info.fh]
            assert ("read", handle, 16, 8) in fx_client.calls
            assert data == bytes(((16 + i) & 0xFF) for i in range(8))
        finally:
            run(fx_ops.release(info.fh))

    def test_read_unknown_fh_returns_ebadf(self, fx_ops):
        _expect_fuse_error(fx_ops.read(9999, 0, 10), errno.EBADF)

    def test_read_propagates_oserror_as_fuseerror(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            fx_client.raise_on_next("read", OSError(errno.EIO, "boom"))
            _expect_fuse_error(fx_ops.read(info.fh, 0, 10), errno.EIO)
        finally:
            run(fx_ops.release(info.fh))


class TestRelease:
    def test_release_dispatches_to_client(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        handle = fx_ops._fh_to_handle[info.fh]
        run(fx_ops.release(info.fh))
        assert ("release", handle) in fx_client.calls

    def test_release_unknown_fh_silent(self, fx_ops):
        run(fx_ops.release(9999))

    def test_release_is_idempotent(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        run(fx_ops.release(info.fh))
        run(fx_ops.release(info.fh))


class TestAccessLogger:
    def test_records_per_read(self, fx_client):
        log = access_log.AccessLogger()
        ops = plink_ops.PlinkOps(fx_client, access_logger=log)
        bed_inode = ops._name_to_inode["small.bed"]
        bim_inode = ops._name_to_inode["small.bim"]
        bed_info = run(ops.open(bed_inode, os.O_RDONLY))
        bim_info = run(ops.open(bim_inode, os.O_RDONLY))
        try:
            run(ops.read(bed_info.fh, 0, 100))
            run(ops.read(bim_info.fh, 0, 50))
        finally:
            run(ops.release(bed_info.fh))
            run(ops.release(bim_info.fh))
        records = log.records
        bed_records = [r for r in records if r.path.endswith(".bed")]
        bim_records = [r for r in records if r.path.endswith(".bim")]
        assert len(bed_records) == 1
        assert len(bim_records) == 1
        assert bed_records[0].offset == 0
        assert bed_records[0].size == 100
        assert bim_records[0].offset == 0
        assert bim_records[0].size == 50


class TestReadOnly:
    def test_access_write_denied(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        _expect_fuse_error(fx_ops.access(inode, os.W_OK), errno.EROFS)

    def test_access_read_allowed(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        run(fx_ops.access(inode, os.R_OK))

    def test_access_unknown_inode(self, fx_ops):
        _expect_fuse_error(fx_ops.access(9999, os.R_OK), errno.ENOENT)


class TestOpendir:
    def test_root(self, fx_ops):
        fh = run(fx_ops.opendir(pyfuse3.ROOT_INODE))
        assert fh == pyfuse3.ROOT_INODE
        run(fx_ops.releasedir(fh))

    def test_non_root_is_notdir(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        _expect_fuse_error(fx_ops.opendir(inode), errno.ENOTDIR)


class TestForget:
    def test_forget_is_noop(self, fx_ops):
        run(fx_ops.forget([(2, 1), (3, 1)]))
