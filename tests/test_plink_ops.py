"""Unit tests for PlinkOps.

Exercises the streaming Operations class via direct async-method calls
— no kernel mount, no subprocess. The vcztools/Zarr-side parity tests
live in ``test_plink_server.py`` (against the server module directly)
and in ``test_plink_client.py`` (against a real subprocess). End-to-end
FUSE behaviour against real plink binaries is in ``test_plink_apps.py``.
"""

import errno
import os
import stat

import pyfuse3
import pytest

from biofuse import access_log, plink_ops


class _FakeBedConnection:
    """In-process stand-in for
    :class:`biofuse.plink_client.BedConnection`.

    Records the call sequence so tests can assert PlinkOps dispatched
    correctly. Reads return deterministic bytes derived from
    ``(offset, size)``.
    """

    def __init__(self, conn_id: int, calls: list[tuple]) -> None:
        self.conn_id = conn_id
        self._calls = calls
        self._closed = False
        self._next_error: OSError | None = None

    def raise_on_next_read(self, exc: OSError) -> None:
        self._next_error = exc

    async def read(self, offset: int, size: int) -> bytes:
        self._calls.append(("read", self.conn_id, offset, size))
        if self._next_error is not None:
            exc = self._next_error
            self._next_error = None
            raise exc
        if self._closed:
            raise OSError(errno.EBADF, "connection closed")
        return bytes(((offset + i) & 0xFF) for i in range(size))

    async def aclose(self) -> None:
        self._calls.append(("aclose", self.conn_id))
        self._closed = True


class _FakeClient:
    """In-process stand-in for :class:`biofuse.plink_client.PlinkClient`.

    Holds canned ``bim_bytes`` / ``fam_bytes`` / ``bed_size`` and hands
    out :class:`_FakeBedConnection` instances on demand.
    """

    def __init__(
        self,
        bim_bytes: bytes = b"BIM" * 64,
        fam_bytes: bytes = b"FAM" * 32,
        bed_size: int = 1024,
    ) -> None:
        self.bim_bytes = bim_bytes
        self.fam_bytes = fam_bytes
        self.bed_size = bed_size
        self.calls: list[tuple] = []
        self._next_open_error: OSError | None = None
        self._next_conn_id = 1
        self.connections: list[_FakeBedConnection] = []

    def raise_on_next_open(self, exc: OSError) -> None:
        self._next_open_error = exc

    async def open_bed(self) -> _FakeBedConnection:
        self.calls.append(("open_bed",))
        if self._next_open_error is not None:
            exc = self._next_open_error
            self._next_open_error = None
            raise exc
        conn = _FakeBedConnection(self._next_conn_id, self.calls)
        self._next_conn_id += 1
        self.connections.append(conn)
        return conn


@pytest.fixture
def fx_client():
    return _FakeClient()


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

    def test_sizes_match_client_metadata(self, fx_client):
        ops = plink_ops.PlinkOps(fx_client, "small")
        sizes = {ops._inode_to_name[i]: size for i, size in ops._inode_to_size.items()}
        assert sizes == {
            "small.bed": fx_client.bed_size,
            "small.bim": len(fx_client.bim_bytes),
            "small.fam": len(fx_client.fam_bytes),
        }

    def test_inodes_assigned_in_sorted_order(self, fx_ops):
        names_in_order = [
            fx_ops._inode_to_name[i] for i in sorted(fx_ops._inode_to_name)
        ]
        assert names_in_order == sorted(names_in_order)


class TestGetattr:
    async def test_root(self, fx_ops):
        attrs = await fx_ops.getattr(pyfuse3.ROOT_INODE)
        assert stat.S_ISDIR(attrs.st_mode)

    async def test_regular_file(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bed"]
        attrs = await fx_ops.getattr(inode)
        assert stat.S_ISREG(attrs.st_mode)
        assert attrs.st_size == fx_client.bed_size

    async def test_unknown_inode(self, fx_ops):
        await _expect_fuse_error(fx_ops.getattr(9999), errno.ENOENT)


class TestLookup:
    async def test_known_name(self, fx_ops, fx_client):
        attrs = await fx_ops.lookup(pyfuse3.ROOT_INODE, b"small.bed")
        assert attrs.st_size == fx_client.bed_size

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
    async def test_bed_open_creates_bed_connection(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bed"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            assert fx_client.calls == [("open_bed",)]
            assert fx_ops._fh_to_kind[info.fh] == "bed"
            assert info.fh in fx_ops._fh_to_conn
        finally:
            await fx_ops.release(info.fh)

    async def test_bim_open_does_not_call_client(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bim"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            assert fx_client.calls == []
            assert fx_ops._fh_to_kind[info.fh] == "bim"
            assert info.fh not in fx_ops._fh_to_conn
        finally:
            await fx_ops.release(info.fh)

    async def test_fam_open_does_not_call_client(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.fam"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            assert fx_client.calls == []
            assert fx_ops._fh_to_kind[info.fh] == "fam"
            assert info.fh not in fx_ops._fh_to_conn
        finally:
            await fx_ops.release(info.fh)

    async def test_each_bed_open_gets_distinct_fh_and_connection(
        self, fx_ops, fx_client
    ):
        inode = fx_ops._name_to_inode["small.bed"]
        info1 = await fx_ops.open(inode, os.O_RDONLY)
        info2 = await fx_ops.open(inode, os.O_RDONLY)
        try:
            assert info1.fh != info2.fh
            assert len(fx_client.connections) == 2
            assert fx_client.connections[0] is not fx_client.connections[1]
        finally:
            await fx_ops.release(info1.fh)
            await fx_ops.release(info2.fh)

    async def test_bed_open_propagates_oserror_as_fuseerror(self, fx_ops, fx_client):
        fx_client.raise_on_next_open(OSError(errno.EACCES, "denied"))
        inode = fx_ops._name_to_inode["small.bed"]
        await _expect_fuse_error(fx_ops.open(inode, os.O_RDONLY), errno.EACCES)


class TestRead:
    async def test_bed_read_dispatches_to_connection(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bed"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            data = await fx_ops.read(info.fh, 16, 8)
            assert ("read", 1, 16, 8) in fx_client.calls
            assert data == bytes(((16 + i) & 0xFF) for i in range(8))
        finally:
            await fx_ops.release(info.fh)

    async def test_bim_read_serves_from_cached_bytes(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bim"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            data = await fx_ops.read(info.fh, 0, len(fx_client.bim_bytes))
            assert data == fx_client.bim_bytes
            # No client traffic for static reads.
            assert all(c[0] != "read" for c in fx_client.calls)
        finally:
            await fx_ops.release(info.fh)

    async def test_fam_read_serves_from_cached_bytes(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.fam"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            data = await fx_ops.read(info.fh, 0, len(fx_client.fam_bytes))
            assert data == fx_client.fam_bytes
        finally:
            await fx_ops.release(info.fh)

    async def test_bim_read_past_eof_returns_empty(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bim"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            data = await fx_ops.read(info.fh, len(fx_client.bim_bytes) + 100, 50)
            assert data == b""
        finally:
            await fx_ops.release(info.fh)

    async def test_bim_read_spanning_eof_returns_truncated(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bim"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            tail_off = len(fx_client.bim_bytes) - 5
            data = await fx_ops.read(info.fh, tail_off, 100)
            assert data == fx_client.bim_bytes[tail_off:]
        finally:
            await fx_ops.release(info.fh)

    async def test_read_unknown_fh_returns_ebadf(self, fx_ops):
        await _expect_fuse_error(fx_ops.read(9999, 0, 10), errno.EBADF)

    async def test_bed_read_propagates_oserror_as_fuseerror(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bed"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            fx_client.connections[0].raise_on_next_read(OSError(errno.EIO, "boom"))
            await _expect_fuse_error(fx_ops.read(info.fh, 0, 10), errno.EIO)
        finally:
            await fx_ops.release(info.fh)


class TestRelease:
    async def test_bed_release_closes_connection(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bed"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        await fx_ops.release(info.fh)
        assert ("aclose", 1) in fx_client.calls
        assert info.fh not in fx_ops._fh_to_conn

    async def test_static_release_does_not_call_client(self, fx_ops, fx_client):
        inode = fx_ops._name_to_inode["small.bim"]
        info = await fx_ops.open(inode, os.O_RDONLY)
        await fx_ops.release(info.fh)
        assert all(c[0] != "aclose" for c in fx_client.calls)

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


class TestStatfs:
    async def test_returns_statvfs_data_with_expected_fields(self, fx_ops):
        out = await fx_ops.statfs()
        assert isinstance(out, pyfuse3.StatvfsData)

    async def test_block_count_matches_sum_of_file_sizes(self, fx_ops, fx_client):
        out = await fx_ops.statfs()
        total = fx_client.bed_size + len(fx_client.bim_bytes) + len(fx_client.fam_bytes)
        assert out.f_bsize > 0
        assert out.f_frsize == out.f_bsize
        assert out.f_blocks == (total + out.f_bsize - 1) // out.f_bsize

    async def test_no_free_space(self, fx_ops):
        out = await fx_ops.statfs()
        assert out.f_bfree == 0
        assert out.f_bavail == 0
        assert out.f_ffree == 0
        assert out.f_favail == 0

    async def test_reports_three_files(self, fx_ops):
        out = await fx_ops.statfs()
        assert out.f_files == 3

    async def test_namemax_is_at_least_posix_min(self, fx_ops):
        out = await fx_ops.statfs()
        assert out.f_namemax >= 255
