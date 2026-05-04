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

BED = bed_protocol.FileType.BED
BIM = bed_protocol.FileType.BIM
FAM = bed_protocol.FileType.FAM


class _FakeClient:
    """In-process stand-in for :class:`biofuse.bed_client.BedEncoderClient`.

    Records the call sequence so tests can assert PlinkOps dispatched
    correctly. Reads return deterministic bytes derived from
    ``(fh, offset, size)``. Errors can be queued via
    :meth:`raise_on_next`.
    """

    def __init__(self, sizes: dict[bed_protocol.FileType, int]) -> None:
        self._sizes = dict(sizes)
        self._open_handles: dict[int, bed_protocol.FileType] = {}
        self.calls: list[tuple] = []
        self._next_error: tuple[str, OSError] | None = None
        # Optional gate: if set, open() awaits this event before returning.
        self.open_gate: trio.Event | None = None

    @property
    def file_entries(self) -> dict[bed_protocol.FileType, int]:
        return dict(self._sizes)

    def raise_on_next(self, op: str, exc: OSError) -> None:
        self._next_error = (op, exc)

    def _maybe_raise(self, op: str) -> None:
        if self._next_error is not None and self._next_error[0] == op:
            _, exc = self._next_error
            self._next_error = None
            raise exc

    async def open(self, fh: int, file_type: bed_protocol.FileType) -> None:
        self.calls.append(("open", fh, file_type))
        self._maybe_raise("open")
        if self.open_gate is not None:
            await self.open_gate.wait()
        self._open_handles[fh] = file_type

    async def read(self, fh: int, offset: int, size: int) -> bytes:
        self.calls.append(("read", fh, offset, size))
        self._maybe_raise("read")
        if fh not in self._open_handles:
            raise OSError(errno.EBADF, "unknown handle")
        return bytes(((offset + i) & 0xFF) for i in range(size))

    async def release(self, fh: int) -> None:
        self.calls.append(("release", fh))
        self._maybe_raise("release")
        self._open_handles.pop(fh, None)


def _default_sizes() -> dict[bed_protocol.FileType, int]:
    return {BED: 1024, BIM: 256, FAM: 100}


@pytest.fixture
def fx_client():
    return _FakeClient(_default_sizes())


@pytest.fixture
def fx_ops(fx_client):
    return plink_ops.PlinkOps(fx_client, "small")


async def _expect_fuse_error(coro, expected_errno):
    with pytest.raises(pyfuse3.FUSEError) as excinfo:
        await coro
    assert excinfo.value.errno == expected_errno


class TestConstructor:
    def test_creates_three_inodes(self, fx_ops):
        names = sorted(fx_ops._name_to_inode)
        assert names == ["small.bed", "small.bim", "small.fam"]

    def test_basenames_propagate(self, fx_client):
        ops = plink_ops.PlinkOps(fx_client, "alt")
        assert sorted(ops._name_to_inode) == ["alt.bed", "alt.bim", "alt.fam"]

    def test_sizes_match_client_entries(self, fx_ops):
        sizes = {
            fx_ops._inode_to_name[i]: size
            for i, size in fx_ops._inode_to_size.items()
        }
        assert sizes == {"small.bed": 1024, "small.bim": 256, "small.fam": 100}

    def test_inodes_assigned_in_sorted_order(self, fx_ops):
        names_in_order = [
            fx_ops._inode_to_name[i] for i in sorted(fx_ops._inode_to_name)
        ]
        assert names_in_order == sorted(names_in_order)


class TestGetattr:
    async def test_root(self, fx_ops):
        attrs = await fx_ops.getattr(pyfuse3.ROOT_INODE)
        assert stat.S_ISDIR(attrs.st_mode)

    async def test_regular_file(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        attrs = await fx_ops.getattr(inode)
        assert stat.S_ISREG(attrs.st_mode)
        assert attrs.st_size == 1024

    async def test_unknown_inode(self, fx_ops):
        await _expect_fuse_error(fx_ops.getattr(9999), errno.ENOENT)


class TestLookup:
    async def test_known_name(self, fx_ops):
        attrs = await fx_ops.lookup(pyfuse3.ROOT_INODE, b"small.bed")
        assert attrs.st_size == 1024

    async def test_unknown_name(self, fx_ops):
        await _expect_fuse_error(
            fx_ops.lookup(pyfuse3.ROOT_INODE, b"nope.bed"), errno.ENOENT
        )

    async def test_invalid_utf8(self, fx_ops):
        await _expect_fuse_error(
            fx_ops.lookup(pyfuse3.ROOT_INODE, b"\xff\xfe.bed"), errno.ENOENT
        )

    async def test_lookup_in_non_root(self, fx_ops):
        await _expect_fuse_error(fx_ops.lookup(2, b"small.bed"), errno.ENOENT)


class TestReaddir:
    async def test_yields_three_entries_in_sorted_order(self, fx_ops):
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
            await fx_ops.readdir(pyfuse3.ROOT_INODE, 0, token)
        finally:
            pyfuse3.readdir_reply = original
        assert [n for n, _ in emitted] == ["small.bed", "small.bim", "small.fam"]

    async def test_readdir_resumes_from_start_id(self, fx_ops):
        emitted: list[str] = []

        def fake_readdir_reply(tok, name, attrs, next_id):
            emitted.append(name.decode("utf-8"))
            return True

        original = pyfuse3.readdir_reply
        pyfuse3.readdir_reply = fake_readdir_reply
        try:
            await fx_ops.readdir(pyfuse3.ROOT_INODE, 1, object())
        finally:
            pyfuse3.readdir_reply = original
        assert emitted == ["small.bim", "small.fam"]


class TestOpenFlags:
    async def test_open_with_write_flag_returns_erofs(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        await _expect_fuse_error(fx_ops.open(inode, os.O_WRONLY), errno.EROFS)

    async def test_open_with_rdwr_flag_returns_erofs(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        await _expect_fuse_error(fx_ops.open(inode, os.O_RDWR), errno.EROFS)

    async def test_open_with_append_flag_returns_erofs(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        await _expect_fuse_error(
            fx_ops.open(inode, os.O_RDONLY | os.O_APPEND), errno.EROFS
        )

    async def test_open_unknown_inode(self, fx_ops):
        await _expect_fuse_error(fx_ops.open(9999, os.O_RDONLY), errno.ENOENT)


class TestOpenDispatch:
    async def test_open_calls_client_with_fh_and_type(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bed"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            assert ("open", info.fh, BED) in fx_client.calls
            assert fx_ops._fh_to_type[info.fh] is BED
        finally:
            await fx_ops.release(info.fh)

    async def test_each_open_gets_distinct_fh(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        info1 = await fx_ops.open(inode, os.O_RDONLY)
        info2 = await fx_ops.open(inode, os.O_RDONLY)
        try:
            assert info1.fh != info2.fh
        finally:
            await fx_ops.release(info1.fh)
            await fx_ops.release(info2.fh)

    async def test_open_propagates_oserror_as_fuseerror(self, fx_ops, fx_client):
        fx_client.raise_on_next("open", OSError(errno.EACCES, "denied"))
        inode = fx_ops._name_to_inode["small.bed"]
        await _expect_fuse_error(fx_ops.open(inode, os.O_RDONLY), errno.EACCES)


class TestRead:
    async def test_read_dispatches_to_client(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bed"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            data = await fx_ops.read(info.fh, 16, 8)
            assert ("read", info.fh, 16, 8) in fx_client.calls
            assert data == bytes(((16 + i) & 0xFF) for i in range(8))
        finally:
            await fx_ops.release(info.fh)

    async def test_read_unknown_fh_returns_ebadf(self, fx_ops):
        await _expect_fuse_error(fx_ops.read(9999, 0, 10), errno.EBADF)

    async def test_read_propagates_oserror_as_fuseerror(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bed"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            fx_client.raise_on_next("read", OSError(errno.EIO, "boom"))
            await _expect_fuse_error(fx_ops.read(info.fh, 0, 10), errno.EIO)
        finally:
            await fx_ops.release(info.fh)


class TestRelease:
    async def test_release_dispatches_to_client(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bed"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        await fx_ops.release(info.fh)
        assert ("release", info.fh) in fx_client.calls

    async def test_release_unknown_fh_silent(self, fx_ops):
        await fx_ops.release(9999)

    async def test_release_is_idempotent(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        await fx_ops.release(info.fh)
        await fx_ops.release(info.fh)


class TestAccessLogger:
    async def test_records_per_read(self, fx_client):
        log = access_log.AccessLogger()
        ops = plink_ops.PlinkOps(fx_client, "small", access_logger=log)
        bed_inode = ops._name_to_inode["small.bed"]
        bim_inode = ops._name_to_inode["small.bim"]
        bed_info = await ops.open(bed_inode, os.O_RDONLY)
        bim_info = await ops.open(bim_inode, os.O_RDONLY)
        try:
            await ops.read(bed_info.fh, 0, 100)
            await ops.read(bim_info.fh, 0, 50)
        finally:
            await ops.release(bed_info.fh)
            await ops.release(bim_info.fh)
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
    async def test_access_write_denied(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        await _expect_fuse_error(fx_ops.access(inode, os.W_OK), errno.EROFS)

    async def test_access_read_allowed(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        await fx_ops.access(inode, os.R_OK)

    async def test_access_unknown_inode(self, fx_ops):
        await _expect_fuse_error(fx_ops.access(9999, os.R_OK), errno.ENOENT)


class TestOpendir:
    async def test_root(self, fx_ops):
        fh = await fx_ops.opendir(pyfuse3.ROOT_INODE)
        assert fh == pyfuse3.ROOT_INODE
        await fx_ops.releasedir(fh)

    async def test_non_root_is_notdir(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        await _expect_fuse_error(fx_ops.opendir(inode), errno.ENOTDIR)


class TestForget:
    async def test_forget_is_noop(self, fx_ops):
        await fx_ops.forget([(2, 1), (3, 1)])


class TestCapacityLimiter:
    """The BED-only capacity limiter throttles concurrent .bed opens
    without blocking .bim/.fam.
    """

    async def test_bed_blocks_at_cap(self, fx_client):
        ops = plink_ops.PlinkOps(fx_client, "small", max_open_bed=2)
        bed_inode = ops._name_to_inode["small.bed"]
        info1 = await ops.open(bed_inode, os.O_RDONLY)
        info2 = await ops.open(bed_inode, os.O_RDONLY)

        third_info = []

        async def third_open():
            info = await ops.open(bed_inode, os.O_RDONLY)
            third_info.append(info)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(third_open)
            await trio.testing.wait_all_tasks_blocked()
            assert third_info == []  # blocked at limiter
            await ops.release(info1.fh)
            # Releasing one slot lets the third open proceed and the
            # nursery exits cleanly.

        assert len(third_info) == 1
        await ops.release(info2.fh)
        await ops.release(third_info[0].fh)

    async def test_bim_does_not_block_when_bed_at_cap(self, fx_client):
        ops = plink_ops.PlinkOps(fx_client, "small", max_open_bed=1)
        bed_inode = ops._name_to_inode["small.bed"]
        bim_inode = ops._name_to_inode["small.bim"]
        bed_info = await ops.open(bed_inode, os.O_RDONLY)
        # cap is 1, BED slot is full; .bim must still proceed.
        bim_info = await ops.open(bim_inode, os.O_RDONLY)
        await ops.release(bim_info.fh)
        await ops.release(bed_info.fh)

    async def test_failed_open_releases_slot(self, fx_client):
        ops = plink_ops.PlinkOps(fx_client, "small", max_open_bed=1)
        bed_inode = ops._name_to_inode["small.bed"]
        # First open fails — slot must be released so the next one can proceed.
        fx_client.raise_on_next("open", OSError(errno.EACCES, "denied"))
        await _expect_fuse_error(ops.open(bed_inode, os.O_RDONLY), errno.EACCES)
        # Second open succeeds (slot was returned).
        info = await ops.open(bed_inode, os.O_RDONLY)
        await ops.release(info.fh)
