"""Unit tests for EncoderOps.

Exercises the streaming Operations class via direct async-method calls
— no kernel mount, no subprocess. The vcztools/Zarr-side parity tests
live in ``test_encoder_server.py`` (against the server module directly)
and in ``test_encoder_client.py`` (against a real subprocess). End-to-end
FUSE behaviour against real plink / bgenix binaries lives in
``test_plink_apps.py`` / ``test_bgen_apps.py``.

Tests parametrise over both :data:`biofuse.formats.PLINK_SPEC` and
:data:`biofuse.formats.BGEN_SPEC` so the spec-driven inode table and
read dispatch are exercised for both output formats.
"""

import errno
import os
import stat
import time

import pyfuse3
import pytest
import trio
import trio.testing
import vcztools

from biofuse import access_log, encoder_ops, formats


def _default_opts(spec):
    if spec.name == "plink":
        return vcztools.ViewPlinkOptions()
    return vcztools.ViewBgenOptions()


class _FakeStreamConnection:
    """In-process stand-in for
    :class:`biofuse.encoder_client.StreamConnection`.

    Records the call sequence so tests can assert EncoderOps dispatched
    correctly. Reads return deterministic bytes derived from
    ``(offset, size)``.
    """

    def __init__(
        self,
        conn_id: int,
        calls: list[tuple],
        on_aclose=None,
    ) -> None:
        self.conn_id = conn_id
        self._calls = calls
        self._closed = False
        self._next_error: OSError | None = None
        self._on_aclose = on_aclose

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
        t0 = time.monotonic()
        self._calls.append(("aclose", self.conn_id))
        self._closed = True
        if self._on_aclose is not None:
            self._on_aclose(t0, time.monotonic())


class _FakeClient:
    """In-process stand-in for :class:`biofuse.encoder_client.EncoderClient`.

    Holds canned ``static_files`` / ``stream_size`` and hands out
    :class:`_FakeStreamConnection` instances on demand.
    """

    def __init__(
        self,
        spec: formats.FormatSpec,
        static_files: dict[str, bytes] | None = None,
        stream_size: int = 1024,
    ) -> None:
        self.spec = spec
        if static_files is None:
            static_files = {
                suffix: b"STATIC" * (32 * (i + 1))
                for i, suffix in enumerate(spec.static_suffixes(_default_opts(spec)))
            }
        self.static_files = static_files
        self.stream_size = stream_size
        self.calls: list[tuple] = []
        self._next_open_error: OSError | None = None
        self._next_conn_id = 1
        self.connections: list[_FakeStreamConnection] = []

    def raise_on_next_open(self, exc: OSError) -> None:
        self._next_open_error = exc

    async def open_stream(self, *, on_aclose=None) -> _FakeStreamConnection:
        self.calls.append(("open_stream",))
        if self._next_open_error is not None:
            exc = self._next_open_error
            self._next_open_error = None
            raise exc
        conn = _FakeStreamConnection(
            self._next_conn_id, self.calls, on_aclose=on_aclose
        )
        self._next_conn_id += 1
        self.connections.append(conn)
        return conn


@pytest.fixture(params=[formats.PLINK_SPEC, formats.BGEN_SPEC], ids=["plink", "bgen"])
def fx_spec(request):
    return request.param


@pytest.fixture
def fx_static_suffixes(fx_spec):
    return fx_spec.static_suffixes(_default_opts(fx_spec))


@pytest.fixture
def fx_client(fx_spec):
    return _FakeClient(fx_spec)


@pytest.fixture
def fx_ops(fx_client, fx_spec):
    return encoder_ops.EncoderOps(fx_client, "small", fx_spec)


@pytest.fixture
def fx_stream_name(fx_spec):
    return f"small{fx_spec.streaming_suffix}"


@pytest.fixture
def fx_first_static_name(fx_static_suffixes):
    return f"small{fx_static_suffixes[0]}"


@pytest.fixture
def fx_first_static_bytes(fx_client, fx_static_suffixes):
    return fx_client.static_files[fx_static_suffixes[0]]


async def _expect_fuse_error(coro, expected_errno):
    with pytest.raises(pyfuse3.FUSEError) as excinfo:
        await coro
    assert excinfo.value.errno == expected_errno


class TestConstructor:
    def test_creates_one_inode_per_manifest_entry(self, fx_ops, fx_static_suffixes):
        n_expected = 1 + len(fx_static_suffixes)
        assert len(fx_ops._name_to_inode) == n_expected

    def test_basename_propagates_to_all_files(
        self, fx_client, fx_spec, fx_static_suffixes
    ):
        ops = encoder_ops.EncoderOps(fx_client, "alt", fx_spec)
        expected = sorted(
            [f"alt{fx_spec.streaming_suffix}"]
            + [f"alt{suffix}" for suffix in fx_static_suffixes]
        )
        assert sorted(ops._name_to_inode) == expected

    def test_sizes_match_client_metadata(self, fx_client, fx_spec, fx_static_suffixes):
        ops = encoder_ops.EncoderOps(fx_client, "small", fx_spec)
        sizes = {ops._inode_to_name[i]: size for i, size in ops._inode_to_size.items()}
        expected = {f"small{fx_spec.streaming_suffix}": fx_client.stream_size}
        for suffix in fx_static_suffixes:
            expected[f"small{suffix}"] = len(fx_client.static_files[suffix])
        assert sizes == expected

    def test_inodes_assigned_in_sorted_order(self, fx_ops):
        names_in_order = [
            fx_ops._inode_to_name[i] for i in sorted(fx_ops._inode_to_name)
        ]
        assert names_in_order == sorted(names_in_order)


class TestGetattr:
    async def test_root(self, fx_ops):
        attrs = await fx_ops.getattr(pyfuse3.ROOT_INODE)
        assert stat.S_ISDIR(attrs.st_mode)

    async def test_streaming_file(self, fx_ops, fx_client, fx_stream_name):
        inode = fx_ops._name_to_inode[fx_stream_name]
        attrs = await fx_ops.getattr(inode)
        assert stat.S_ISREG(attrs.st_mode)
        assert attrs.st_size == fx_client.stream_size

    async def test_unknown_inode(self, fx_ops):
        await _expect_fuse_error(fx_ops.getattr(9999), errno.ENOENT)


class TestLookup:
    async def test_known_name(self, fx_ops, fx_client, fx_stream_name):
        attrs = await fx_ops.lookup(pyfuse3.ROOT_INODE, fx_stream_name.encode("utf-8"))
        assert attrs.st_size == fx_client.stream_size

    async def test_unknown_name(self, fx_ops):
        await _expect_fuse_error(
            fx_ops.lookup(pyfuse3.ROOT_INODE, b"nope.xyz"), errno.ENOENT
        )

    async def test_invalid_utf8(self, fx_ops):
        await _expect_fuse_error(
            fx_ops.lookup(pyfuse3.ROOT_INODE, b"\xff\xfe.bed"), errno.ENOENT
        )

    async def test_lookup_in_non_root(self, fx_ops, fx_stream_name):
        await _expect_fuse_error(
            fx_ops.lookup(2, fx_stream_name.encode("utf-8")), errno.ENOENT
        )


class TestReaddir:
    async def test_yields_entries_in_sorted_order(
        self, fx_ops, fx_spec, fx_static_suffixes
    ):
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
        expected = sorted(
            [f"small{fx_spec.streaming_suffix}"]
            + [f"small{suffix}" for suffix in fx_static_suffixes]
        )
        assert [n for n, _ in emitted] == expected


class TestOpenFlags:
    async def test_open_with_write_flag_returns_erofs(self, fx_ops, fx_stream_name):
        inode = fx_ops._name_to_inode[fx_stream_name]
        await _expect_fuse_error(fx_ops.open(inode, os.O_WRONLY), errno.EROFS)

    async def test_open_with_rdwr_flag_returns_erofs(self, fx_ops, fx_stream_name):
        inode = fx_ops._name_to_inode[fx_stream_name]
        await _expect_fuse_error(fx_ops.open(inode, os.O_RDWR), errno.EROFS)

    async def test_open_with_append_flag_returns_erofs(self, fx_ops, fx_stream_name):
        inode = fx_ops._name_to_inode[fx_stream_name]
        await _expect_fuse_error(
            fx_ops.open(inode, os.O_RDONLY | os.O_APPEND), errno.EROFS
        )

    async def test_open_unknown_inode(self, fx_ops):
        await _expect_fuse_error(fx_ops.open(9999, os.O_RDONLY), errno.ENOENT)


class TestOpenDispatch:
    async def test_stream_open_creates_connection(
        self, fx_ops, fx_client, fx_spec, fx_stream_name
    ):
        inode = fx_ops._name_to_inode[fx_stream_name]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            assert fx_client.calls == [("open_stream",)]
            assert fx_ops._fh_to_kind[info.fh] == fx_spec.streaming_kind
            assert info.fh in fx_ops._fh_to_conn
        finally:
            await fx_ops.release(info.fh)

    async def test_static_open_does_not_call_client(
        self, fx_ops, fx_client, fx_first_static_name
    ):
        inode = fx_ops._name_to_inode[fx_first_static_name]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            assert fx_client.calls == []
            assert fx_ops._fh_to_kind[info.fh] == "static"
            assert info.fh not in fx_ops._fh_to_conn
        finally:
            await fx_ops.release(info.fh)

    async def test_each_stream_open_gets_distinct_fh_and_connection(
        self, fx_ops, fx_client, fx_stream_name
    ):
        inode = fx_ops._name_to_inode[fx_stream_name]
        info1 = await fx_ops.open(inode, os.O_RDONLY)
        info2 = await fx_ops.open(inode, os.O_RDONLY)
        try:
            assert info1.fh != info2.fh
            assert len(fx_client.connections) == 2
            assert fx_client.connections[0] is not fx_client.connections[1]
        finally:
            await fx_ops.release(info1.fh)
            await fx_ops.release(info2.fh)

    async def test_stream_open_propagates_oserror_as_fuseerror(
        self, fx_ops, fx_client, fx_stream_name
    ):
        fx_client.raise_on_next_open(OSError(errno.EACCES, "denied"))
        inode = fx_ops._name_to_inode[fx_stream_name]
        await _expect_fuse_error(fx_ops.open(inode, os.O_RDONLY), errno.EACCES)


class TestRead:
    async def test_stream_read_dispatches_to_connection(
        self, fx_ops, fx_client, fx_stream_name
    ):
        inode = fx_ops._name_to_inode[fx_stream_name]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            data = await fx_ops.read(info.fh, 16, 8)
            assert ("read", 1, 16, 8) in fx_client.calls
            assert data == bytes(((16 + i) & 0xFF) for i in range(8))
        finally:
            await fx_ops.release(info.fh)

    async def test_static_read_serves_from_cached_bytes(
        self, fx_ops, fx_client, fx_first_static_name, fx_first_static_bytes
    ):
        inode = fx_ops._name_to_inode[fx_first_static_name]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            data = await fx_ops.read(info.fh, 0, len(fx_first_static_bytes))
            assert data == fx_first_static_bytes
            # No client traffic for static reads.
            assert all(c[0] != "read" for c in fx_client.calls)
        finally:
            await fx_ops.release(info.fh)

    async def test_static_read_past_eof_returns_empty(
        self, fx_ops, fx_first_static_name, fx_first_static_bytes
    ):
        inode = fx_ops._name_to_inode[fx_first_static_name]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            data = await fx_ops.read(info.fh, len(fx_first_static_bytes) + 100, 50)
            assert data == b""
        finally:
            await fx_ops.release(info.fh)

    async def test_static_read_spanning_eof_returns_truncated(
        self, fx_ops, fx_first_static_name, fx_first_static_bytes
    ):
        inode = fx_ops._name_to_inode[fx_first_static_name]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            tail_off = len(fx_first_static_bytes) - 5
            data = await fx_ops.read(info.fh, tail_off, 100)
            assert data == fx_first_static_bytes[tail_off:]
        finally:
            await fx_ops.release(info.fh)

    async def test_read_unknown_fh_returns_ebadf(self, fx_ops):
        await _expect_fuse_error(fx_ops.read(9999, 0, 10), errno.EBADF)

    async def test_stream_read_propagates_oserror_as_fuseerror(
        self, fx_ops, fx_client, fx_stream_name
    ):
        inode = fx_ops._name_to_inode[fx_stream_name]
        info = await fx_ops.open(inode, os.O_RDONLY)
        try:
            fx_client.connections[0].raise_on_next_read(OSError(errno.EIO, "boom"))
            await _expect_fuse_error(fx_ops.read(info.fh, 0, 10), errno.EIO)
        finally:
            await fx_ops.release(info.fh)


class TestRelease:
    async def test_stream_release_closes_connection(
        self, fx_ops, fx_client, fx_stream_name
    ):
        inode = fx_ops._name_to_inode[fx_stream_name]
        info = await fx_ops.open(inode, os.O_RDONLY)
        await fx_ops.release(info.fh)
        assert ("aclose", 1) in fx_client.calls
        assert info.fh not in fx_ops._fh_to_conn

    async def test_static_release_does_not_call_client(
        self, fx_ops, fx_client, fx_first_static_name
    ):
        inode = fx_ops._name_to_inode[fx_first_static_name]
        info = await fx_ops.open(inode, os.O_RDONLY)
        await fx_ops.release(info.fh)
        assert all(c[0] != "aclose" for c in fx_client.calls)

    async def test_release_unknown_fh_silent(self, fx_ops):
        await fx_ops.release(9999)

    async def test_release_is_idempotent(self, fx_ops, fx_stream_name):
        inode = fx_ops._name_to_inode[fx_stream_name]
        info = await fx_ops.open(inode, os.O_RDONLY)
        await fx_ops.release(info.fh)
        await fx_ops.release(info.fh)


class TestAccessLogger:
    async def test_records_per_read(self, fx_client, fx_spec, fx_static_suffixes):
        log = access_log.AccessLogger()
        ops = encoder_ops.EncoderOps(fx_client, "small", fx_spec, access_logger=log)
        stream_inode = ops._name_to_inode[f"small{fx_spec.streaming_suffix}"]
        static_inode = ops._name_to_inode[f"small{fx_static_suffixes[0]}"]
        stream_info = await ops.open(stream_inode, os.O_RDONLY)
        static_info = await ops.open(static_inode, os.O_RDONLY)
        try:
            await ops.read(stream_info.fh, 0, 100)
            await ops.read(static_info.fh, 0, 50)
        finally:
            await ops.release(stream_info.fh)
            await ops.release(static_info.fh)
        records = [r for r in log.records if r.kind == "read"]
        stream_records = [
            r for r in records if r.path.endswith(fx_spec.streaming_suffix)
        ]
        static_records = [r for r in records if r.path.endswith(fx_static_suffixes[0])]
        assert len(stream_records) == 1
        assert len(static_records) == 1
        assert stream_records[0].offset == 0
        assert stream_records[0].size == 100
        assert static_records[0].offset == 0
        assert static_records[0].size == 50


class TestCapacityLimiter:
    """The streaming-only capacity limiter throttles concurrent
    streaming-file opens without blocking static reads.
    """

    async def test_stream_blocks_at_cap(self, fx_client, fx_spec):
        ops = encoder_ops.EncoderOps(fx_client, "small", fx_spec, max_open_stream=2)
        stream_inode = ops._name_to_inode[f"small{fx_spec.streaming_suffix}"]
        info1 = await ops.open(stream_inode, os.O_RDONLY)
        info2 = await ops.open(stream_inode, os.O_RDONLY)

        third_info: list[pyfuse3.FileInfo] = []

        async def third_open():
            info = await ops.open(stream_inode, os.O_RDONLY)
            third_info.append(info)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(third_open)
            await trio.testing.wait_all_tasks_blocked()
            assert third_info == []  # blocked at limiter
            await ops.release(info1.fh)

        assert len(third_info) == 1
        await ops.release(info2.fh)
        await ops.release(third_info[0].fh)

    async def test_static_does_not_block_when_stream_at_cap(
        self, fx_client, fx_spec, fx_static_suffixes
    ):
        ops = encoder_ops.EncoderOps(fx_client, "small", fx_spec, max_open_stream=1)
        stream_inode = ops._name_to_inode[f"small{fx_spec.streaming_suffix}"]
        static_inode = ops._name_to_inode[f"small{fx_static_suffixes[0]}"]
        stream_info = await ops.open(stream_inode, os.O_RDONLY)
        static_info = await ops.open(static_inode, os.O_RDONLY)
        await ops.release(static_info.fh)
        await ops.release(stream_info.fh)

    async def test_failed_open_releases_slot(self, fx_client, fx_spec):
        ops = encoder_ops.EncoderOps(fx_client, "small", fx_spec, max_open_stream=1)
        stream_inode = ops._name_to_inode[f"small{fx_spec.streaming_suffix}"]
        fx_client.raise_on_next_open(OSError(errno.EACCES, "denied"))
        await _expect_fuse_error(ops.open(stream_inode, os.O_RDONLY), errno.EACCES)
        info = await ops.open(stream_inode, os.O_RDONLY)
        await ops.release(info.fh)

    async def test_open_returns_eagain_when_limiter_starved(
        self, fx_client, fx_spec, monkeypatch
    ):
        """A leaked limiter slot must not pin FUSE_OPEN forever.

        With the cap held by an existing open and the slot never
        released, a competing open must surface ``EAGAIN`` once the
        per-mount limiter deadline expires."""
        monkeypatch.setattr(encoder_ops, "_LIMITER_TIMEOUT_S", 0.2)
        ops = encoder_ops.EncoderOps(fx_client, "small", fx_spec, max_open_stream=1)
        stream_inode = ops._name_to_inode[f"small{fx_spec.streaming_suffix}"]
        held = await ops.open(stream_inode, os.O_RDONLY)
        try:
            await _expect_fuse_error(ops.open(stream_inode, os.O_RDONLY), errno.EAGAIN)
        finally:
            await ops.release(held.fh)

    async def test_limiter_timeout_records_access_event(
        self, fx_client, fx_spec, monkeypatch
    ):
        """A timed-out FUSE_OPEN must leave a ``limiter_timeout`` event
        in the access log so post-hoc analysis can attribute an
        ``EAGAIN`` to limiter starvation rather than some other path."""
        timeout_s = 0.2
        monkeypatch.setattr(encoder_ops, "_LIMITER_TIMEOUT_S", timeout_s)
        log = access_log.AccessLogger()
        ops = encoder_ops.EncoderOps(
            fx_client, "small", fx_spec, max_open_stream=1, access_logger=log
        )
        stream_inode = ops._name_to_inode[f"small{fx_spec.streaming_suffix}"]
        held = await ops.open(stream_inode, os.O_RDONLY)
        try:
            await _expect_fuse_error(ops.open(stream_inode, os.O_RDONLY), errno.EAGAIN)
        finally:
            await ops.release(held.fh)
        timeouts = [r for r in log.records if r.kind == "limiter_timeout"]
        assert len(timeouts) == 1
        rec = timeouts[0]
        # The held open ran first and got fh=1; the timed-out attempt is
        # the next allocated fh.
        assert rec.fh == held.fh + 1
        assert rec.t_end - rec.t_start >= timeout_s


class TestLifecycleEvents:
    """``open`` / ``release`` / ``aclose`` / ``limiter_wait`` events
    are emitted on the access logger so we can localise where time is
    spent in the lifecycle without changing the read trace."""

    async def test_stream_emits_full_lifecycle(self, fx_client, fx_spec):
        log = access_log.AccessLogger()
        ops = encoder_ops.EncoderOps(fx_client, "small", fx_spec, access_logger=log)
        stream_inode = ops._name_to_inode[f"small{fx_spec.streaming_suffix}"]
        info = await ops.open(stream_inode, os.O_RDONLY)
        await ops.read(info.fh, 0, 8)
        await ops.release(info.fh)
        kinds = [r.kind for r in log.records]
        assert "limiter_wait" in kinds
        assert "open" in kinds
        assert "read" in kinds
        assert "release" in kinds
        assert "aclose" in kinds
        # `release` and `aclose` should both be tied to the same fh.
        rel = next(r for r in log.records if r.kind == "release")
        acl = next(r for r in log.records if r.kind == "aclose")
        assert rel.fh == info.fh
        assert acl.fh == info.fh

    async def test_static_emits_open_and_release_only(
        self, fx_client, fx_spec, fx_static_suffixes
    ):
        log = access_log.AccessLogger()
        ops = encoder_ops.EncoderOps(fx_client, "small", fx_spec, access_logger=log)
        static_inode = ops._name_to_inode[f"small{fx_static_suffixes[0]}"]
        info = await ops.open(static_inode, os.O_RDONLY)
        await ops.release(info.fh)
        kinds = [r.kind for r in log.records]
        assert kinds == ["open", "release"]


class TestReadOnly:
    async def test_access_write_denied(self, fx_ops, fx_stream_name):
        inode = fx_ops._name_to_inode[fx_stream_name]
        await _expect_fuse_error(fx_ops.access(inode, os.W_OK), errno.EROFS)

    async def test_access_read_allowed(self, fx_ops, fx_stream_name):
        inode = fx_ops._name_to_inode[fx_stream_name]
        await fx_ops.access(inode, os.R_OK)

    async def test_access_unknown_inode(self, fx_ops):
        await _expect_fuse_error(fx_ops.access(9999, os.R_OK), errno.ENOENT)


class TestOpendir:
    async def test_root(self, fx_ops):
        fh = await fx_ops.opendir(pyfuse3.ROOT_INODE)
        assert fh == pyfuse3.ROOT_INODE
        await fx_ops.releasedir(fh)

    async def test_non_root_is_notdir(self, fx_ops, fx_stream_name):
        inode = fx_ops._name_to_inode[fx_stream_name]
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
        total = fx_client.stream_size + sum(
            len(b) for b in fx_client.static_files.values()
        )
        assert out.f_bsize > 0
        assert out.f_frsize == out.f_bsize
        assert out.f_blocks == (total + out.f_bsize - 1) // out.f_bsize

    async def test_no_free_space(self, fx_ops):
        out = await fx_ops.statfs()
        assert out.f_bfree == 0
        assert out.f_bavail == 0
        assert out.f_ffree == 0
        assert out.f_favail == 0

    async def test_reports_manifest_files(self, fx_ops, fx_static_suffixes):
        out = await fx_ops.statfs()
        assert out.f_files == 1 + len(fx_static_suffixes)

    async def test_namemax_is_at_least_posix_min(self, fx_ops):
        out = await fx_ops.statfs()
        assert out.f_namemax >= 255
