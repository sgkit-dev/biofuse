"""End-to-end tests against real plink1.9 / plink2 binaries.

These tests:
1. Materialise a "golden" plink fileset with vcztools.plink.write_plink.
2. Mount the same VCZ via biofuse and run the same plink command against
   the mount.
3. Compare the resulting plink output files byte-for-byte (or, where plink
   embeds timestamps, structurally).

All tests in this file require the plink binaries to exist on PATH; they
are skipped otherwise. CI installs them; local dev machines need them too.

Tests are async because :func:`fuse_adapter.mount` runs ``pyfuse3.main`` in
the same trio event loop as the test body. Using sync ``subprocess.run``
or ``time.sleep`` would block FUSE request servicing and deadlock plink.
``trio.run_process`` and ``trio.sleep`` are the trio-native equivalents.
"""

import os
import pathlib
import shutil

import pytest
import trio
from vcztools.cli import make_reader
from vcztools.plink import write_plink

from biofuse import access_log, encoder_client, encoder_ops, formats, fuse_adapter

PLINK1 = shutil.which("plink1.9") or shutil.which("plink")
PLINK2 = shutil.which("plink2")

needs_plink1 = pytest.mark.skipif(PLINK1 is None, reason="plink1.9 not installed")
needs_plink2 = pytest.mark.skipif(PLINK2 is None, reason="plink2 not installed")


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
async def fx_mounted_plink(tmp_path, fx_medium_vcz):
    """Mount the medium VCZ as a plink fileset via biofuse.

    Yields ``(mnt, basename, golden_dir, log)`` where ``golden_dir`` holds a
    directly-materialised version of the same fileset for byte-comparison.
    """
    mnt = tmp_path / "mnt"
    mnt.mkdir()
    golden = tmp_path / "golden_dir"
    golden.mkdir()

    write_plink(make_reader(str(fx_medium_vcz.path)), golden / "medium")

    log = access_log.AccessLogger()
    sock_path = tmp_path / "plink.sock"
    async with await encoder_client.EncoderClient.start(
        str(fx_medium_vcz.path), sock_path, formats.PLINK_SPEC
    ) as client:
        ops = encoder_ops.EncoderOps(
            client, "medium", formats.PLINK_SPEC, access_logger=log
        )
        async with fuse_adapter.mount(ops, str(mnt)):
            await _wait_for_mount(mnt)
            yield mnt, "medium", golden, log


async def _arun(cmd) -> None:
    await trio.run_process(cmd, capture_stdout=True, capture_stderr=True, check=True)


@needs_plink1
class TestPlinkOneNine:
    async def test_freq_matches_golden(self, fx_mounted_plink, tmp_path):
        mnt, basename, golden, _ = fx_mounted_plink
        out_mnt = tmp_path / "freq_mnt"
        out_gld = tmp_path / "freq_gld"
        await _arun(
            [PLINK1, "--bfile", str(mnt / basename), "--freq", "--out", str(out_mnt)]
        )
        await _arun(
            [PLINK1, "--bfile", str(golden / basename), "--freq", "--out", str(out_gld)]
        )
        assert (
            out_mnt.with_suffix(".frq").read_bytes()
            == out_gld.with_suffix(".frq").read_bytes()
        )

    async def test_missing_matches_golden(self, fx_mounted_plink, tmp_path):
        mnt, basename, golden, _ = fx_mounted_plink
        out_mnt = tmp_path / "miss_mnt"
        out_gld = tmp_path / "miss_gld"
        await _arun(
            [PLINK1, "--bfile", str(mnt / basename), "--missing", "--out", str(out_mnt)]
        )
        await _arun(
            [
                PLINK1,
                "--bfile",
                str(golden / basename),
                "--missing",
                "--out",
                str(out_gld),
            ]
        )
        for ext in (".lmiss", ".imiss"):
            mnt_out = pathlib.Path(str(out_mnt) + ext)
            gld_out = pathlib.Path(str(out_gld) + ext)
            assert mnt_out.read_bytes() == gld_out.read_bytes(), f"differs at {ext}"

    async def test_hardy_matches_golden(self, fx_mounted_plink, tmp_path):
        mnt, basename, golden, _ = fx_mounted_plink
        out_mnt = tmp_path / "hardy_mnt"
        out_gld = tmp_path / "hardy_gld"
        await _arun(
            [PLINK1, "--bfile", str(mnt / basename), "--hardy", "--out", str(out_mnt)]
        )
        await _arun(
            [
                PLINK1,
                "--bfile",
                str(golden / basename),
                "--hardy",
                "--out",
                str(out_gld),
            ]
        )
        assert (
            out_mnt.with_suffix(".hwe").read_bytes()
            == out_gld.with_suffix(".hwe").read_bytes()
        )

    async def test_repeated_invocations(self, fx_mounted_plink, tmp_path):
        """Running plink twice in a row against the same mount must succeed."""
        mnt, basename, _, _ = fx_mounted_plink
        for i in range(2):
            out = tmp_path / f"repeat_{i}"
            await _arun(
                [PLINK1, "--bfile", str(mnt / basename), "--freq", "--out", str(out)]
            )
            assert (out.with_suffix(".frq")).exists()


@needs_plink2
class TestPlinkTwo:
    async def test_freq_matches_golden(self, fx_mounted_plink, tmp_path):
        """plink2 --freq on the mount must equal --freq on the golden directory."""
        mnt, basename, golden, _ = fx_mounted_plink
        out_mnt = tmp_path / "p2_freq_mnt"
        out_gld = tmp_path / "p2_freq_gld"
        await _arun(
            [PLINK2, "--bfile", str(mnt / basename), "--freq", "--out", str(out_mnt)]
        )
        await _arun(
            [PLINK2, "--bfile", str(golden / basename), "--freq", "--out", str(out_gld)]
        )
        assert (
            out_mnt.with_suffix(".afreq").read_bytes()
            == out_gld.with_suffix(".afreq").read_bytes()
        )


@needs_plink1
class TestParallelClient:
    """Runs plink against the mount while a parallel reader tails .bim."""

    async def test_python_read_during_plink(self, fx_mounted_plink, tmp_path):
        mnt, basename, _, _ = fx_mounted_plink
        bim_path = mnt / f"{basename}.bim"
        n_iters = 50
        errors: list[str] = []
        first = await trio.to_thread.run_sync(bim_path.read_bytes)

        async def reader():
            for _ in range(n_iters):
                got = await trio.to_thread.run_sync(bim_path.read_bytes)
                if got != first:
                    errors.append(f"bim differs: {len(got)} vs {len(first)}")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(reader)
            out = tmp_path / "parallel"
            await _arun(
                [PLINK1, "--bfile", str(mnt / basename), "--freq", "--out", str(out)]
            )
        assert errors == []


class TestStatfs:
    """End-to-end check that ``statvfs(2)`` against the mount returns
    sensible values rather than ENOSYS.
    """

    async def test_statfs_via_os_statvfs(self, fx_mounted_plink):
        mnt, _, _, _ = fx_mounted_plink
        # os.statvfs syscalls into the FUSE mount; if we ran it on the
        # trio thread that's also serving FUSE requests we'd deadlock,
        # so route it through a worker thread.
        info = await trio.to_thread.run_sync(os.statvfs, str(mnt))
        assert info.f_bsize > 0
        assert info.f_blocks > 0
        # Read-only, fixed-size FS: nothing free.
        assert info.f_bavail == 0
        assert info.f_ffree == 0
        assert info.f_files == 3
        assert info.f_namemax >= 255


@needs_plink1
class TestAccessTrace:
    """Captures access patterns for inspection in phase-2 design."""

    async def test_freq_produces_traces(self, fx_mounted_plink, tmp_path):
        mnt, basename, _, log = fx_mounted_plink
        out = tmp_path / "trace_target"
        await _arun(
            [PLINK1, "--bfile", str(mnt / basename), "--freq", "--out", str(out)]
        )

        records = log.records
        assert len(records) > 0
        bed_records = [r for r in records if r.path.endswith(".bed")]
        bim_records = [r for r in records if r.path.endswith(".bim")]
        fam_records = [r for r in records if r.path.endswith(".fam")]
        assert len(bed_records) > 0, "expected reads against .bed"
        assert len(bim_records) > 0, "expected reads against .bim"
        assert len(fam_records) > 0, "expected reads against .fam"

        if bed_records:
            min_off = min(r.offset for r in bed_records)
            max_end = max(r.offset + r.size for r in bed_records)
            assert min_off == 0
            assert max_end > 0
