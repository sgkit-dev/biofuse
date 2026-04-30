"""End-to-end tests against real plink1.9 / plink2 binaries.

These tests:
1. Materialise a "golden" plink fileset with vcztools.plink.write_plink.
2. Mount the same VCZ via biofuse and run the same plink command against
   the mount.
3. Compare the resulting plink output files byte-for-byte (or, where plink
   embeds timestamps, structurally).

All tests in this file require the plink binaries to exist on PATH; they
are skipped otherwise. CI installs them; local dev machines need them too.
"""

import os
import pathlib
import shutil
import subprocess
import threading
import time

import pytest
from vcztools.cli import make_reader
from vcztools.plink import write_plink

from biofuse import access_log, fuse_adapter, passthrough_view, plink_source

PLINK1 = shutil.which("plink1.9") or shutil.which("plink")
PLINK2 = shutil.which("plink2")

needs_plink1 = pytest.mark.skipif(PLINK1 is None, reason="plink1.9 not installed")
needs_plink2 = pytest.mark.skipif(PLINK2 is None, reason="plink2 not installed")


def _wait_for_mount(mnt: pathlib.Path, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.ismount(mnt):
            return
        time.sleep(0.02)
    raise RuntimeError(f"mountpoint {mnt} not live after {timeout}s")


@pytest.fixture
def fx_mounted_plink(tmp_path, fx_medium_vcz):
    """Mount the medium VCZ as a plink fileset via biofuse.

    Yields ``(mnt, basename, golden_dir)`` where ``golden_dir`` is a sibling
    directory holding a directly-materialised version of the same fileset
    for comparison.
    """
    mnt = tmp_path / "mnt"
    mnt.mkdir()
    golden = tmp_path / "golden_dir"
    golden.mkdir()

    write_plink(make_reader(str(fx_medium_vcz.path)), golden / "medium")

    log = access_log.AccessLogger()
    source = plink_source.PlinkSource(fx_medium_vcz.path)
    backing = source.open()
    view = passthrough_view.PassthroughDirectoryView(backing, access_logger=log)
    mount = fuse_adapter.Mount(view, str(mnt))
    mount.__enter__()
    try:
        _wait_for_mount(mnt)
        yield mnt, "medium", golden, log
    finally:
        mount.__exit__(None, None, None)
        view.close()
        source.close()


def _run(cmd, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, check=True, **kwargs)


@needs_plink1
class TestPlinkOneNine:
    def test_freq_matches_golden(self, fx_mounted_plink, tmp_path):
        mnt, basename, golden, _ = fx_mounted_plink
        out_mnt = tmp_path / "freq_mnt"
        out_gld = tmp_path / "freq_gld"
        _run([PLINK1, "--bfile", str(mnt / basename), "--freq", "--out", str(out_mnt)])
        _run(
            [PLINK1, "--bfile", str(golden / basename), "--freq", "--out", str(out_gld)]
        )
        assert (
            out_mnt.with_suffix(".frq").read_bytes()
            == out_gld.with_suffix(".frq").read_bytes()
        )

    def test_missing_matches_golden(self, fx_mounted_plink, tmp_path):
        mnt, basename, golden, _ = fx_mounted_plink
        out_mnt = tmp_path / "miss_mnt"
        out_gld = tmp_path / "miss_gld"
        _run(
            [PLINK1, "--bfile", str(mnt / basename), "--missing", "--out", str(out_mnt)]
        )
        _run(
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

    def test_hardy_matches_golden(self, fx_mounted_plink, tmp_path):
        mnt, basename, golden, _ = fx_mounted_plink
        out_mnt = tmp_path / "hardy_mnt"
        out_gld = tmp_path / "hardy_gld"
        _run([PLINK1, "--bfile", str(mnt / basename), "--hardy", "--out", str(out_mnt)])
        _run(
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

    def test_repeated_invocations(self, fx_mounted_plink, tmp_path):
        """Running plink twice in a row against the same mount must succeed."""
        mnt, basename, _, _ = fx_mounted_plink
        for i in range(2):
            out = tmp_path / f"repeat_{i}"
            _run([PLINK1, "--bfile", str(mnt / basename), "--freq", "--out", str(out)])
            assert (out.with_suffix(".frq")).exists()


@needs_plink2
class TestPlinkTwo:
    def test_freq_matches_golden(self, fx_mounted_plink, tmp_path):
        """plink2 --freq on the mount must equal --freq on the golden directory."""
        mnt, basename, golden, _ = fx_mounted_plink
        out_mnt = tmp_path / "p2_freq_mnt"
        out_gld = tmp_path / "p2_freq_gld"
        _run([PLINK2, "--bfile", str(mnt / basename), "--freq", "--out", str(out_mnt)])
        _run(
            [PLINK2, "--bfile", str(golden / basename), "--freq", "--out", str(out_gld)]
        )
        assert (
            out_mnt.with_suffix(".afreq").read_bytes()
            == out_gld.with_suffix(".afreq").read_bytes()
        )


@needs_plink1
class TestParallelClient:
    """Runs plink against the mount while a parallel Python reader tails .bim."""

    def test_python_read_during_plink(self, fx_mounted_plink, tmp_path):
        mnt, basename, _, _ = fx_mounted_plink
        bim_path = mnt / f"{basename}.bim"
        n_iters = 50
        errors: list[str] = []
        first = bim_path.read_bytes()

        def reader():
            for _ in range(n_iters):
                got = bim_path.read_bytes()
                if got != first:
                    errors.append(f"bim differs: {len(got)} vs {len(first)}")

        t = threading.Thread(target=reader)
        t.start()
        out = tmp_path / "parallel"
        _run([PLINK1, "--bfile", str(mnt / basename), "--freq", "--out", str(out)])
        t.join()
        assert errors == []


@needs_plink1
class TestAccessTrace:
    """Captures access patterns for inspection in phase-2 design."""

    def test_freq_produces_traces(self, fx_mounted_plink, tmp_path):
        mnt, basename, _, log = fx_mounted_plink
        out = tmp_path / "trace_target"
        _run([PLINK1, "--bfile", str(mnt / basename), "--freq", "--out", str(out)])

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
