"""Integration tests against a real FUSE mount.

Each test mounts a small backing directory in a tmpdir and exercises it from
the same Python process via os/pathlib. pyfuse3's mount state is process-
global, so these tests must NOT run with multi-process xdist in a single
worker — pytest-xdist ships them across workers, but each worker only ever
holds one mount at a time, which is fine.
"""

import errno
import os
import pathlib
import random
import subprocess
import threading
import time

import pytest
from vcztools.cli import make_reader
from vcztools.plink import write_plink

from biofuse import access_log, fuse_adapter, passthrough_view


@pytest.fixture
def fx_backing(tmp_path):
    """A flat backing directory with three files of distinct content."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "alpha.bed").write_bytes(bytes(range(256)) * 4)  # 1024 bytes
    (src / "alpha.bim").write_text("chr1\trs1\t0\t100\tA\tG\n" * 50)
    (src / "alpha.fam").write_text("0\tS1\t0\t0\t0\t-9\n" * 10)
    return src


def _wait_for_mount(mnt: pathlib.Path, timeout: float = 5.0) -> None:
    """Wait for FUSE mount to become live, polling with short backoff."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.ismount(mnt):
            return
        time.sleep(0.02)
    raise RuntimeError(f"mountpoint {mnt} not live after {timeout}s")


@pytest.fixture
def fx_mount(tmp_path, fx_backing):
    """Yields the mount path with backing fx_backing live throughout the test."""
    mnt = tmp_path / "mnt"
    mnt.mkdir()
    view = passthrough_view.PassthroughDirectoryView(fx_backing)
    ops = fuse_adapter.BiofuseOperations(view)
    mount = fuse_adapter.Mount(ops, str(mnt))
    mount.__enter__()
    try:
        _wait_for_mount(mnt)
        yield mnt, view
    finally:
        mount.__exit__(None, None, None)
        view.close()


class TestBasicAccess:
    def test_listing_matches_backing(self, fx_mount, fx_backing):
        mnt, _ = fx_mount
        assert sorted(p.name for p in mnt.iterdir()) == sorted(
            p.name for p in fx_backing.iterdir()
        )

    def test_stat_size_matches_backing(self, fx_mount, fx_backing):
        mnt, _ = fx_mount
        for child in fx_backing.iterdir():
            assert (mnt / child.name).stat().st_size == child.stat().st_size

    def test_stat_mode_is_readonly(self, fx_mount):
        mnt, _ = fx_mount
        for path in mnt.iterdir():
            mode = path.stat().st_mode
            assert mode & 0o222 == 0, f"{path} has write bits set"

    def test_full_read_matches_backing(self, fx_mount, fx_backing):
        mnt, _ = fx_mount
        for child in fx_backing.iterdir():
            assert (mnt / child.name).read_bytes() == child.read_bytes()


class TestReadPatterns:
    def test_byte_at_a_time(self, fx_mount, fx_backing):
        mnt, _ = fx_mount
        original = (fx_backing / "alpha.bed").read_bytes()
        with (mnt / "alpha.bed").open("rb") as f:
            for offset in range(len(original)):
                f.seek(offset)
                got = f.read(1)
                assert got == original[offset : offset + 1]

    @pytest.mark.parametrize("block_size", [1, 3, 7, 13, 4093, 65537])
    def test_prime_block_sizes(self, fx_mount, fx_backing, block_size):
        mnt, _ = fx_mount
        original = (fx_backing / "alpha.bed").read_bytes()
        chunks = []
        with (mnt / "alpha.bed").open("rb") as f:
            while True:
                chunk = f.read(block_size)
                if not chunk:
                    break
                chunks.append(chunk)
        assert b"".join(chunks) == original

    def test_random_pread_offsets(self, fx_mount, fx_backing):
        mnt, _ = fx_mount
        original = (fx_backing / "alpha.bed").read_bytes()
        rng = random.Random(7)
        fd = os.open(mnt / "alpha.bed", os.O_RDONLY)
        try:
            for _ in range(200):
                offset = rng.randrange(len(original) + 100)
                size = rng.randrange(1, 256)
                got = os.pread(fd, size, offset)
                assert got == original[offset : offset + size]
        finally:
            os.close(fd)

    def test_read_at_eof_returns_empty(self, fx_mount, fx_backing):
        mnt, _ = fx_mount
        size = (fx_backing / "alpha.bed").stat().st_size
        fd = os.open(mnt / "alpha.bed", os.O_RDONLY)
        try:
            assert os.pread(fd, 100, size) == b""
            assert os.pread(fd, 100, size + 1_000_000) == b""
        finally:
            os.close(fd)

    def test_read_spanning_eof(self, fx_mount, fx_backing):
        mnt, _ = fx_mount
        original = (fx_backing / "alpha.bed").read_bytes()
        size = len(original)
        fd = os.open(mnt / "alpha.bed", os.O_RDONLY)
        try:
            assert os.pread(fd, 100, size - 10) == original[size - 10 :]
        finally:
            os.close(fd)


class TestReopen:
    def test_reopen_sees_same_bytes(self, fx_mount, fx_backing):
        mnt, _ = fx_mount
        a = (mnt / "alpha.bed").read_bytes()
        b = (mnt / "alpha.bed").read_bytes()
        assert a == b == (fx_backing / "alpha.bed").read_bytes()

    def test_concurrent_handles(self, fx_mount, fx_backing):
        mnt, _ = fx_mount
        original = (fx_backing / "alpha.bed").read_bytes()
        fd1 = os.open(mnt / "alpha.bed", os.O_RDONLY)
        fd2 = os.open(mnt / "alpha.bed", os.O_RDONLY)
        try:
            assert os.pread(fd1, 100, 0) == original[:100]
            assert os.pread(fd2, 100, 500) == original[500:600]
            assert os.pread(fd1, 100, 200) == original[200:300]
        finally:
            os.close(fd1)
            os.close(fd2)


class TestThreadedReaders:
    def test_many_threads_full_scans(self, fx_mount, fx_backing):
        mnt, _ = fx_mount
        original = (fx_backing / "alpha.bed").read_bytes()
        n_threads = 8
        errors: list[str] = []

        def worker():
            for _ in range(5):
                got = (mnt / "alpha.bed").read_bytes()
                if got != original:
                    errors.append(f"mismatch: got {len(got)} bytes")

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []


class TestReadOnly:
    def test_write_returns_eacces_or_erofs(self, fx_mount):
        mnt, _ = fx_mount
        with pytest.raises(OSError, match=".*") as exc_info:
            (mnt / "alpha.bed").write_bytes(b"x")
        # EACCES for write to read-only-mode file, EROFS for read-only mount.
        assert exc_info.value.errno in (errno.EACCES, errno.EROFS)

    def test_create_returns_eacces_or_erofs(self, fx_mount):
        mnt, _ = fx_mount
        with pytest.raises(OSError, match=".*") as exc_info:
            (mnt / "newfile").write_text("x")
        assert exc_info.value.errno in (errno.EACCES, errno.EROFS, errno.ENOSYS)

    def test_unlink_returns_appropriate_errno(self, fx_mount):
        mnt, _ = fx_mount
        with pytest.raises(OSError, match=".*") as exc_info:
            (mnt / "alpha.bed").unlink()
        assert exc_info.value.errno in (
            errno.EACCES,
            errno.EROFS,
            errno.ENOSYS,
            errno.EPERM,
        )


class TestMissingNames:
    def test_stat_unknown_raises_enoent(self, fx_mount):
        mnt, _ = fx_mount
        with pytest.raises(FileNotFoundError):
            (mnt / "nope.bed").stat()

    def test_open_unknown_raises_enoent(self, fx_mount):
        mnt, _ = fx_mount
        with pytest.raises(FileNotFoundError):
            (mnt / "nope.bed").read_bytes()


class TestAccessLogger:
    def test_records_reads_through_kernel(self, tmp_path, fx_backing):
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        log = access_log.AccessLogger()
        view = passthrough_view.PassthroughDirectoryView(fx_backing, access_logger=log)
        ops = fuse_adapter.BiofuseOperations(view)
        with fuse_adapter.Mount(ops, str(mnt)):
            _wait_for_mount(mnt)
            (mnt / "alpha.bed").read_bytes()
        view.close()
        records = log.records
        assert len(records) > 0
        assert all(r.path == "alpha.bed" for r in records)
        total = sum(r.size for r in records)
        assert total == (fx_backing / "alpha.bed").stat().st_size


class TestDirectIO:
    """Mounts with direct_io=True should bypass the kernel page cache.

    Every byte a consumer touches must surface through the access logger,
    including bytes accessed via mmap (or mmap is refused, in which case
    the consumer falls back to read() and we still see every byte).
    """

    def test_basic_read_still_works(self, tmp_path, fx_backing):
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        view = passthrough_view.PassthroughDirectoryView(fx_backing)
        ops = fuse_adapter.BiofuseOperations(view, direct_io=True)
        with fuse_adapter.Mount(ops, str(mnt)):
            _wait_for_mount(mnt)
            assert (mnt / "alpha.bed").read_bytes() == (
                fx_backing / "alpha.bed"
            ).read_bytes()
        view.close()

    def test_mmap_reads_surface_through_access_log(self, tmp_path, fx_backing):
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        log = access_log.AccessLogger()
        view = passthrough_view.PassthroughDirectoryView(
            fx_backing, access_logger=log
        )
        original = (fx_backing / "alpha.bed").read_bytes()
        ops = fuse_adapter.BiofuseOperations(view, direct_io=True)
        with fuse_adapter.Mount(ops, str(mnt)):
            _wait_for_mount(mnt)
            # Run mmap in a child process so the page-fault path is the
            # same kernel codepath that a real consumer (e.g. bed-reader)
            # would hit, and so a deadlock (FUSE thread blocked on its
            # own page fault) cannot wedge the test runner.
            script = (
                "import mmap, sys\n"
                "with open(sys.argv[1], 'rb') as f:\n"
                "    try:\n"
                "        mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)\n"
                "    except OSError as exc:\n"
                "        sys.stdout.write(f'mmap-failed: {exc.errno}\\n')\n"
                "        sys.exit(0)\n"
                "    sys.stdout.write(f'mmap-ok: {mm[:].hex()}\\n')\n"
                "    mm.close()\n"
            )
            result = subprocess.run(
                ["python3", "-c", script, str(mnt / "alpha.bed")],
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
            )
        view.close()
        out = result.stdout.strip()
        if out.startswith("mmap-failed"):
            # mmap fully blocked is also acceptable — same outcome from
            # biofuse's perspective: the consumer cannot evade the logger.
            return
        assert out.startswith("mmap-ok"), out
        got_hex = out.split(": ", 1)[1]
        assert bytes.fromhex(got_hex) == original
        # Every byte the consumer "saw" must have come through FUSE.
        bytes_logged = sum(r.size for r in log.records if r.path == "alpha.bed")
        assert bytes_logged >= len(original)


class TestMountLifecycle:
    def test_mountpoint_reports_fusefs_during_mount(self, fx_mount):
        mnt, _ = fx_mount
        assert os.path.ismount(mnt)

    def test_mountpoint_no_longer_mount_after_close(self, tmp_path, fx_backing):
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        view = passthrough_view.PassthroughDirectoryView(fx_backing)
        ops = fuse_adapter.BiofuseOperations(view)
        with fuse_adapter.Mount(ops, str(mnt)):
            _wait_for_mount(mnt)
            assert os.path.ismount(mnt)
        assert not os.path.ismount(mnt)
        view.close()

    def test_double_mount_in_same_process_rejected(self, tmp_path, fx_backing):
        mnt1 = tmp_path / "mnt1"
        mnt2 = tmp_path / "mnt2"
        mnt1.mkdir()
        mnt2.mkdir()
        view1 = passthrough_view.PassthroughDirectoryView(fx_backing)
        view2 = passthrough_view.PassthroughDirectoryView(fx_backing)
        ops1 = fuse_adapter.BiofuseOperations(view1)
        ops2 = fuse_adapter.BiofuseOperations(view2)
        with fuse_adapter.Mount(ops1, str(mnt1)):
            _wait_for_mount(mnt1)
            with pytest.raises(RuntimeError, match="another biofuse Mount"):
                with fuse_adapter.Mount(ops2, str(mnt2)):
                    pass
        view1.close()
        view2.close()


class TestCatSubprocess:
    """Cross-process consumer using `cat`. Closer to real client behaviour."""

    def test_cat_full_file(self, fx_mount, fx_backing):
        mnt, _ = fx_mount
        result = subprocess.run(
            ["cat", str(mnt / "alpha.bed")],
            capture_output=True,
            check=True,
        )
        assert result.stdout == (fx_backing / "alpha.bed").read_bytes()

    def test_head_terminates_cleanly(self, fx_mount, fx_backing):
        mnt, _ = fx_mount
        # head closes the pipe early, simulating a reader that gives up.
        result = subprocess.run(
            f"cat {mnt / 'alpha.bed'} | head -c 100",
            shell=True,
            capture_output=True,
            check=True,
        )
        original = (fx_backing / "alpha.bed").read_bytes()
        assert result.stdout == original[:100]

    def test_cat_after_early_termination(self, fx_mount, fx_backing):
        mnt, _ = fx_mount
        subprocess.run(
            f"cat {mnt / 'alpha.bed'} | head -c 50",
            shell=True,
            capture_output=True,
            check=True,
        )
        result = subprocess.run(
            ["cat", str(mnt / "alpha.bed")],
            capture_output=True,
            check=True,
        )
        assert result.stdout == (fx_backing / "alpha.bed").read_bytes()


class TestVczGoldenParity:
    """End-to-end: VCZ → write_plink → mount → byte-compare via FUSE."""

    def test_bytes_match_direct_write(self, tmp_path, fx_small_vcz):
        backing = tmp_path / "backing"
        backing.mkdir()
        prefix = backing / "small"
        write_plink(make_reader(str(fx_small_vcz.path)), prefix)

        mnt = tmp_path / "mnt"
        mnt.mkdir()
        view = passthrough_view.PassthroughDirectoryView(backing)
        ops = fuse_adapter.BiofuseOperations(view)
        with fuse_adapter.Mount(ops, str(mnt)):
            _wait_for_mount(mnt)
            for suffix in (".bed", ".bim", ".fam"):
                got = (mnt / f"small{suffix}").read_bytes()
                expected = pathlib.Path(str(prefix) + suffix).read_bytes()
                assert got == expected, f"mismatch for {suffix}"
        view.close()
