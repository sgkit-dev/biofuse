"""End-to-end tests for BGEN mounts.

Mirrors :mod:`test_plink_apps`: mount a VCZ as a BGEN fileset via the
real encoder-server subprocess, then read the mounted files. Where
external binaries (``bgenix``, ``qctool``) are on PATH, the relevant
test classes also exercise the mount through those tools.
"""

import os
import pathlib
import shutil
import sqlite3

import pytest
import trio
from vcztools.bgen import BgenEncoder
from vcztools.cli import make_reader

from biofuse import access_log, encoder_client, encoder_ops, formats, fuse_adapter

BGENIX = shutil.which("bgenix")
QCTOOL = shutil.which("qctool")

needs_bgenix = pytest.mark.skipif(BGENIX is None, reason="bgenix not installed")
needs_qctool = pytest.mark.skipif(QCTOOL is None, reason="qctool not installed")


async def _wait_for_mount(mnt: pathlib.Path, timeout: float = 5.0) -> None:
    """Block until ``mnt`` is a live FUSE mount.

    ``os.path.ismount`` makes a sync ``lstat`` call into the FUSE mount,
    which the kernel forwards to the userspace pyfuse3 daemon — the
    same trio thread that's calling ismount. A direct call would
    deadlock; ``trio.to_thread.run_sync`` runs the syscall on a worker
    thread so the trio loop stays free to serve the FUSE request.
    """
    deadline = trio.current_time() + timeout
    while trio.current_time() < deadline:
        live = await trio.to_thread.run_sync(os.path.ismount, str(mnt))
        if live:
            return
        await trio.sleep(0.02)
    raise RuntimeError(f"mountpoint {mnt} not live after {timeout}s")


@pytest.fixture
async def fx_mounted_bgen(tmp_path, fx_medium_vcz):
    """Mount the medium VCZ as a BGEN fileset via biofuse.

    Yields ``(mnt, basename, expected_bytes, log)`` where ``expected_bytes``
    is the full streaming-file bytes the encoder would emit (computed
    once via an in-process ``BgenEncoder`` on a fresh reader). We do
    not compare against ``vcztools.bgen.write_bgen`` output because
    ``write_bgen`` defaults to compressed BGEN (smaller, non-fixed-size)
    while biofuse's encoder always uses level 0 (fixed-size for random
    access).
    """
    mnt = tmp_path / "mnt"
    mnt.mkdir()
    expected = _encoder_bytes(fx_medium_vcz.path)

    log = access_log.AccessLogger()
    sock_path = tmp_path / "bgen.sock"
    async with await encoder_client.EncoderClient.start(
        str(fx_medium_vcz.path), sock_path, formats.BGEN_SPEC
    ) as client:
        ops = encoder_ops.EncoderOps(
            client, "medium", formats.BGEN_SPEC, access_logger=log
        )
        async with fuse_adapter.mount(ops, str(mnt)):
            await _wait_for_mount(mnt)
            yield mnt, "medium", expected, log


def _encoder_bytes(vcz_path: pathlib.Path) -> bytes:
    reader = make_reader(str(vcz_path))
    with BgenEncoder(reader) as enc:
        return enc.read(0, enc.total_size)


async def _arun(cmd) -> None:
    await trio.run_process(cmd, capture_stdout=True, capture_stderr=True, check=True)


class TestBgenBytes:
    async def test_full_bgen_matches_encoder(self, fx_mounted_bgen):
        mnt, basename, expected, _ = fx_mounted_bgen
        data = await trio.to_thread.run_sync((mnt / f"{basename}.bgen").read_bytes)
        assert data == expected

    async def test_sample_text_well_formed(self, fx_mounted_bgen):
        mnt, basename, _, _ = fx_mounted_bgen
        text = await trio.to_thread.run_sync((mnt / f"{basename}.sample").read_text)
        lines = text.splitlines()
        # Header (ID_1 ID_2 missing), column-type row (0 0 0), then one row
        # per sample.
        assert lines[0].split() == ["ID_1", "ID_2", "missing"]
        assert lines[1].split() == ["0", "0", "0"]
        assert len(lines) >= 3

    async def test_bgi_parses_as_sqlite(self, fx_mounted_bgen, fx_medium_vcz):
        mnt, basename, _, _ = fx_mounted_bgen
        bgi_bytes = await trio.to_thread.run_sync(
            (mnt / f"{basename}.bgen.bgi").read_bytes
        )
        # ``sqlite3.connect`` needs a filesystem path, so spill the
        # bytes to a tmp file in the test scope.
        bgi_path = mnt.parent / f"{basename}.bgen.bgi.copy"
        bgi_path.write_bytes(bgi_bytes)
        conn = sqlite3.connect(str(bgi_path))
        try:
            count = conn.execute("SELECT COUNT(*) FROM Variant").fetchone()[0]
        finally:
            conn.close()
        assert count == fx_medium_vcz.num_variants


class TestBgenSeekable:
    async def test_random_pread(self, fx_mounted_bgen):
        """Random-offset reads from the mounted .bgen match the encoder's
        full-bytes view at the same window."""
        mnt, basename, expected, _ = fx_mounted_bgen
        bgen_path = mnt / f"{basename}.bgen"
        # Every syscall against the FUSE mount must run on a worker
        # thread; the trio thread itself is busy serving FUSE requests.
        # See test_plink_apps._wait_for_mount for the same rationale.
        for off, size in [
            (0, 32),
            (len(expected) // 3, 1024),
            (len(expected) - 17, 100),
        ]:
            got = await trio.to_thread.run_sync(_pread_sync, bgen_path, off, size)
            assert got == expected[off : off + size]


def _pread_sync(path: pathlib.Path, off: int, size: int) -> bytes:
    with path.open("rb") as f:
        f.seek(off)
        return f.read(size)


@needs_bgenix
class TestBgenix:
    async def test_bgenix_lists_variants(
        self, fx_mounted_bgen, fx_medium_vcz, tmp_path
    ):
        mnt, basename, _, _ = fx_mounted_bgen
        # ``bgenix -g X -list`` writes a per-variant table to stdout.
        result = await trio.run_process(
            [BGENIX, "-g", str(mnt / f"{basename}.bgen"), "-list"],
            capture_stdout=True,
            capture_stderr=True,
            check=True,
        )
        stdout_text = result.stdout.decode()
        # Each variant produces one row; sum of data rows must match
        # the variant count (header / trailer lines start with '#').
        data_rows = [
            ln
            for ln in stdout_text.splitlines()
            if ln and not ln.startswith("#") and not ln.startswith("alternate_ids")
        ]
        assert len(data_rows) == fx_medium_vcz.num_variants


@needs_qctool
class TestQctool:
    async def test_qctool_snp_stats_match_encoder_bytes(
        self, fx_mounted_bgen, tmp_path
    ):
        """``qctool -snp-stats`` on the mount equals the same call against
        a side-by-side copy of the encoder's bytes (written to a plain
        file). Pins that the mount is byte-equivalent for qctool's
        consumption."""
        mnt, basename, expected, _ = fx_mounted_bgen
        plain = tmp_path / "plain.bgen"
        plain.write_bytes(expected)
        # The biofuse mount needs the .sample to be co-located for
        # qctool to associate samples with the BGEN; copy ours over.
        await trio.to_thread.run_sync(
            shutil.copyfile,
            mnt / f"{basename}.sample",
            tmp_path / "plain.sample",
        )
        out_mnt = tmp_path / "snp_mnt"
        out_plain = tmp_path / "snp_plain"
        await _arun(
            [
                QCTOOL,
                "-g",
                str(mnt / f"{basename}.bgen"),
                "-s",
                str(mnt / f"{basename}.sample"),
                "-snp-stats",
                "-osnp",
                str(out_mnt),
            ]
        )
        await _arun(
            [
                QCTOOL,
                "-g",
                str(plain),
                "-s",
                str(tmp_path / "plain.sample"),
                "-snp-stats",
                "-osnp",
                str(out_plain),
            ]
        )
        assert out_mnt.read_bytes() == out_plain.read_bytes()


class TestStatfs:
    """End-to-end check that ``statvfs(2)`` against the mount returns
    sensible values rather than ENOSYS.
    """

    async def test_statfs_via_os_statvfs(self, fx_mounted_bgen):
        mnt, _, _, _ = fx_mounted_bgen
        info = await trio.to_thread.run_sync(os.statvfs, str(mnt))
        assert info.f_bsize > 0
        assert info.f_blocks > 0
        assert info.f_bavail == 0
        assert info.f_ffree == 0
        assert info.f_files == 3
        assert info.f_namemax >= 255


class TestAccessTrace:
    """Captures access patterns for inspection in phase-2 design."""

    async def test_read_produces_traces(self, fx_mounted_bgen):
        mnt, basename, _, log = fx_mounted_bgen
        await trio.to_thread.run_sync((mnt / f"{basename}.bgen").read_bytes)
        await trio.to_thread.run_sync((mnt / f"{basename}.sample").read_bytes)
        await trio.to_thread.run_sync((mnt / f"{basename}.bgen.bgi").read_bytes)

        records = log.records
        assert len(records) > 0
        bgen_records = [r for r in records if r.path.endswith(".bgen")]
        sample_records = [r for r in records if r.path.endswith(".sample")]
        bgi_records = [r for r in records if r.path.endswith(".bgen.bgi")]
        assert len(bgen_records) > 0, "expected reads against .bgen"
        assert len(sample_records) > 0, "expected reads against .sample"
        assert len(bgi_records) > 0, "expected reads against .bgen.bgi"

        if bgen_records:
            min_off = min(r.offset for r in bgen_records)
            max_end = max(r.offset + r.size for r in bgen_records)
            assert min_off == 0
            assert max_end > 0
