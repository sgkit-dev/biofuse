"""Unit tests for PlinkOps.

Exercises the streaming Operations class via direct async-method calls — no
kernel mount. End-to-end FUSE behaviour against real plink binaries is in
tests/test_plink_apps.py.
"""

import errno
import os
import pathlib
import stat

import pyfuse3
import pytest
import trio
from vcztools.cli import make_reader
from vcztools.plink import write_plink

from biofuse import access_log, plink_ops


def run(coro):
    return trio.run(lambda: coro)


@pytest.fixture
def fx_golden_dir(tmp_path, fx_small_vcz):
    """A directory containing the directly-materialised PLINK fileset for
    fx_small_vcz, used as a byte-identity reference."""
    golden = tmp_path / "golden"
    golden.mkdir()
    write_plink(make_reader(str(fx_small_vcz.path)), golden / "small")
    return golden, "small"


@pytest.fixture
def fx_reader(fx_small_vcz):
    return make_reader(str(fx_small_vcz.path))


@pytest.fixture
def fx_ops(fx_reader):
    return plink_ops.PlinkOps(fx_reader, "small")


def _expect_fuse_error(coro, expected_errno):
    with pytest.raises(pyfuse3.FUSEError) as excinfo:
        run(coro)
    assert excinfo.value.errno == expected_errno


class TestStaticBytesFile:
    def test_full_read(self):
        f = plink_ops._StaticBytesFile("x.bim", b"hello world")
        assert f.read(0, 100) == b"hello world"

    def test_partial_read(self):
        f = plink_ops._StaticBytesFile("x.bim", b"hello world")
        assert f.read(6, 5) == b"world"

    def test_size_property(self):
        f = plink_ops._StaticBytesFile("x.bim", b"hello world")
        assert f.size == 11
        assert f.name == "x.bim"

    def test_read_past_eof_returns_empty(self):
        f = plink_ops._StaticBytesFile("x.bim", b"abc")
        assert f.read(3, 10) == b""
        assert f.read(100, 10) == b""

    def test_read_zero_size_returns_empty(self):
        f = plink_ops._StaticBytesFile("x.bim", b"abc")
        assert f.read(0, 0) == b""

    def test_negative_offset_raises(self):
        f = plink_ops._StaticBytesFile("x.bim", b"abc")
        with pytest.raises(ValueError):
            f.read(-1, 10)

    def test_negative_size_raises(self):
        f = plink_ops._StaticBytesFile("x.bim", b"abc")
        with pytest.raises(ValueError):
            f.read(0, -1)

    def test_close_is_idempotent(self):
        f = plink_ops._StaticBytesFile("x.bim", b"abc")
        f.close()
        f.close()

    def test_read_after_close_raises(self):
        f = plink_ops._StaticBytesFile("x.bim", b"abc")
        f.close()
        with pytest.raises(RuntimeError):
            f.read(0, 1)


class TestConstructor:
    def test_creates_three_inodes(self, fx_ops):
        assert len(fx_ops._inode_to_entry) == 3
        names = sorted(fx_ops._name_to_inode)
        assert names == ["small.bed", "small.bim", "small.fam"]

    def test_basename_propagates_to_filenames(self, fx_reader):
        ops = plink_ops.PlinkOps(fx_reader, "alt_name")
        names = sorted(ops._name_to_inode)
        assert names == ["alt_name.bed", "alt_name.bim", "alt_name.fam"]

    def test_bed_size_matches_formula(self, fx_ops, fx_small_vcz):
        bed_inode = fx_ops._name_to_inode["small.bed"]
        bed_entry = fx_ops._inode_to_entry[bed_inode]
        bytes_per_variant = (fx_small_vcz.num_samples + 3) // 4
        assert bed_entry.size == 3 + fx_small_vcz.num_variants * bytes_per_variant

    def test_bim_fam_sizes_match_golden(self, fx_ops, fx_golden_dir):
        golden, basename = fx_golden_dir
        for ext in (".bim", ".fam"):
            entry = fx_ops._inode_to_entry[fx_ops._name_to_inode[f"{basename}{ext}"]]
            expected_size = (golden / f"{basename}{ext}").stat().st_size
            assert entry.size == expected_size

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
        assert attrs.st_size > 0

    def test_unknown_inode(self, fx_ops):
        _expect_fuse_error(fx_ops.getattr(9999), errno.ENOENT)


class TestLookup:
    def test_known_name(self, fx_ops, fx_golden_dir):
        golden, _ = fx_golden_dir
        attrs = run(fx_ops.lookup(pyfuse3.ROOT_INODE, b"small.bed"))
        assert attrs.st_size == (golden / "small.bed").stat().st_size

    def test_unknown_name(self, fx_ops):
        _expect_fuse_error(
            fx_ops.lookup(pyfuse3.ROOT_INODE, b"nope.bed"), errno.ENOENT
        )

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

        import pyfuse3 as _pyfuse3

        original = _pyfuse3.readdir_reply
        _pyfuse3.readdir_reply = fake_readdir_reply
        try:
            run(fx_ops.readdir(pyfuse3.ROOT_INODE, 0, token))
        finally:
            _pyfuse3.readdir_reply = original
        assert [n for n, _ in emitted] == ["small.bed", "small.bim", "small.fam"]

    def test_readdir_resumes_from_start_id(self, fx_ops):
        emitted: list[str] = []

        def fake_readdir_reply(tok, name, attrs, next_id):
            emitted.append(name.decode("utf-8"))
            return True

        import pyfuse3 as _pyfuse3

        original = _pyfuse3.readdir_reply
        _pyfuse3.readdir_reply = fake_readdir_reply
        try:
            run(fx_ops.readdir(pyfuse3.ROOT_INODE, 1, object()))
        finally:
            _pyfuse3.readdir_reply = original
        # entry_id 1 was the first entry (small.bed); resuming after it should
        # emit only the remaining two.
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
    def test_bed_open_uses_bed_encoder(self, fx_ops):
        from vcztools import plink as vcztools_plink

        inode = fx_ops._name_to_inode["small.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            backend = fx_ops._open_files[info.fh]
            assert isinstance(backend, vcztools_plink.BedEncoder)
        finally:
            run(fx_ops.release(info.fh))

    def test_bim_open_uses_static_bytes(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bim"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            backend = fx_ops._open_files[info.fh]
            assert isinstance(backend, plink_ops._StaticBytesFile)
        finally:
            run(fx_ops.release(info.fh))

    def test_fam_open_uses_static_bytes(self, fx_ops):
        inode = fx_ops._name_to_inode["small.fam"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            backend = fx_ops._open_files[info.fh]
            assert isinstance(backend, plink_ops._StaticBytesFile)
        finally:
            run(fx_ops.release(info.fh))


class TestBedReadParity:
    """The full .bed read through PlinkOps must equal the golden write_plink
    output byte-for-byte."""

    def test_full_sequential_read(self, fx_ops, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        inode = fx_ops._name_to_inode[f"{basename}.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            data = run(fx_ops.read(info.fh, 0, len(expected) * 2))
            assert data == expected
        finally:
            run(fx_ops.release(info.fh))

    @pytest.mark.parametrize("block_size", [1, 7, 13, 4096, 65536])
    def test_chunked_sequential_read(self, fx_ops, fx_golden_dir, block_size):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        inode = fx_ops._name_to_inode[f"{basename}.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            chunks = []
            offset = 0
            while True:
                data = run(fx_ops.read(info.fh, offset, block_size))
                if not data:
                    break
                chunks.append(data)
                offset += len(data)
            assert b"".join(chunks) == expected
        finally:
            run(fx_ops.release(info.fh))

    def test_random_pread(self, fx_ops, fx_golden_dir):
        import random

        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        inode = fx_ops._name_to_inode[f"{basename}.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        rng = random.Random(11)
        try:
            for _ in range(50):
                offset = rng.randrange(len(expected))
                size = rng.randrange(1, 64)
                got = run(fx_ops.read(info.fh, offset, size))
                assert got == expected[offset : offset + size]
        finally:
            run(fx_ops.release(info.fh))

    def test_read_past_eof_returns_empty(self, fx_ops, fx_golden_dir):
        golden, basename = fx_golden_dir
        bed_size = (golden / f"{basename}.bed").stat().st_size
        inode = fx_ops._name_to_inode[f"{basename}.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            assert run(fx_ops.read(info.fh, bed_size, 100)) == b""
            assert run(fx_ops.read(info.fh, bed_size + 10_000, 100)) == b""
        finally:
            run(fx_ops.release(info.fh))


class TestBimFamReadParity:
    def test_bim_full_match(self, fx_ops, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bim").read_bytes()
        inode = fx_ops._name_to_inode[f"{basename}.bim"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            assert run(fx_ops.read(info.fh, 0, len(expected) * 2)) == expected
        finally:
            run(fx_ops.release(info.fh))

    def test_fam_full_match(self, fx_ops, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.fam").read_bytes()
        inode = fx_ops._name_to_inode[f"{basename}.fam"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            assert run(fx_ops.read(info.fh, 0, len(expected) * 2)) == expected
        finally:
            run(fx_ops.release(info.fh))


class TestConcurrentHandles:
    def test_two_bed_encoders_independent(self, fx_ops, fx_golden_dir):
        """Two open .bed handles must yield independent iterator state — reads
        from one must not perturb the other."""
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        inode = fx_ops._name_to_inode[f"{basename}.bed"]
        info1 = run(fx_ops.open(inode, os.O_RDONLY))
        info2 = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            assert info1.fh != info2.fh
            half = len(expected) // 2
            assert run(fx_ops.read(info1.fh, 0, half)) == expected[:half]
            assert run(fx_ops.read(info2.fh, half, half)) == expected[half : 2 * half]
            assert run(fx_ops.read(info1.fh, half, half)) == expected[half : 2 * half]
            assert run(fx_ops.read(info2.fh, 0, half)) == expected[:half]
        finally:
            run(fx_ops.release(info1.fh))
            run(fx_ops.release(info2.fh))


class TestReopen:
    def test_reopen_after_release(self, fx_ops, fx_golden_dir):
        golden, basename = fx_golden_dir
        expected = (golden / f"{basename}.bed").read_bytes()
        inode = fx_ops._name_to_inode[f"{basename}.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        run(fx_ops.read(info.fh, 0, 100))
        run(fx_ops.release(info.fh))

        info2 = run(fx_ops.open(inode, os.O_RDONLY))
        try:
            assert run(fx_ops.read(info2.fh, 0, len(expected) * 2)) == expected
        finally:
            run(fx_ops.release(info2.fh))


class TestRead:
    def test_read_unknown_handle(self, fx_ops):
        _expect_fuse_error(fx_ops.read(9999, 0, 10), errno.EBADF)

    def test_release_unknown_handle_silent(self, fx_ops):
        run(fx_ops.release(9999))

    def test_release_is_idempotent(self, fx_ops):
        inode = fx_ops._name_to_inode["small.bed"]
        info = run(fx_ops.open(inode, os.O_RDONLY))
        run(fx_ops.release(info.fh))
        run(fx_ops.release(info.fh))


class TestAccessLogger:
    def test_records_per_read(self, fx_reader, fx_golden_dir):
        golden, basename = fx_golden_dir
        log = access_log.AccessLogger()
        ops = plink_ops.PlinkOps(fx_reader, basename, access_logger=log)
        bed_inode = ops._name_to_inode[f"{basename}.bed"]
        bim_inode = ops._name_to_inode[f"{basename}.bim"]
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
