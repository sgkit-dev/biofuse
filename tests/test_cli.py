"""Tests for the biofuse CLI."""

import os
import pathlib
import signal
import subprocess
import sys
import time

from click.testing import CliRunner

from biofuse import cli


class TestHelp:
    def test_top_level_help(self):
        result = CliRunner().invoke(cli.biofuse_main, ["--help"])
        assert result.exit_code == 0
        assert "mount-plink" in result.output
        assert "mount-bgen" in result.output

    def test_mount_plink_help(self):
        result = CliRunner().invoke(cli.biofuse_main, ["mount-plink", "--help"])
        assert result.exit_code == 0
        assert "VCZ_URL" in result.output
        assert "MOUNT_DIR" in result.output
        # The ViewPlinkOptions.decorator pulls in the bcftools-style
        # filter group + the sidecar toggles.
        for flag in [
            "--regions",
            "--samples",
            "--max-alleles",
            "--no-bim",
            "--no-fam",
            "--log-level",
        ]:
            assert flag in result.output, f"missing {flag} in mount-plink help"

    def test_mount_bgen_help(self):
        result = CliRunner().invoke(cli.biofuse_main, ["mount-bgen", "--help"])
        assert result.exit_code == 0
        assert "VCZ_URL" in result.output
        assert "MOUNT_DIR" in result.output
        for flag in [
            "--regions",
            "--samples",
            "--no-sample-file",
            "--no-bgi",
            "--no-header-samples",
            "--compression-level",
            "--log-level",
        ]:
            assert flag in result.output, f"missing {flag} in mount-bgen help"

    def test_version(self):
        result = CliRunner().invoke(cli.biofuse_main, ["--version"])
        assert result.exit_code == 0


class TestArgumentValidation:
    def test_missing_args_fails(self):
        result = CliRunner().invoke(cli.biofuse_main, ["mount-plink"])
        assert result.exit_code != 0

    def test_missing_mount_dir_fails(self):
        result = CliRunner().invoke(cli.biofuse_main, ["mount-plink", "x.vcz"])
        assert result.exit_code != 0

    def test_nonexistent_mount_dir_fails(self, tmp_path):
        result = CliRunner().invoke(
            cli.biofuse_main,
            ["mount-plink", "x.vcz", str(tmp_path / "missing-mount")],
        )
        assert result.exit_code != 0
        assert "mount directory does not exist" in result.output

    def test_bgen_nonexistent_mount_dir_fails(self, tmp_path):
        result = CliRunner().invoke(
            cli.biofuse_main,
            ["mount-bgen", "x.vcz", str(tmp_path / "missing-mount")],
        )
        assert result.exit_code != 0
        assert "mount directory does not exist" in result.output


class TestEndToEndMount:
    """Spawn the CLI as a subprocess, wait for mount, read files, terminate."""

    def test_mounts_and_serves_files(self, tmp_path, fx_small_vcz):
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "biofuse.cli",
                "mount-plink",
                str(fx_small_vcz.path),
                str(mnt),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            self._wait_for_mount(mnt, proc, timeout=15)
            names = sorted(p.name for p in mnt.iterdir())
            assert "small.bed" in names
            assert "small.bim" in names
            assert "small.fam" in names
            assert (mnt / "small.bed").stat().st_size > 0
            assert (mnt / "small.fam").read_text().count("\n") == 10
        finally:
            self._terminate(proc, mnt)

    def test_access_log_written(self, tmp_path, fx_small_vcz):
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        log_path = tmp_path / "trace.jsonl"
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "biofuse.cli",
                "mount-plink",
                str(fx_small_vcz.path),
                str(mnt),
                "--access-log",
                str(log_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            self._wait_for_mount(mnt, proc, timeout=15)
            (mnt / "small.bed").read_bytes()
            (mnt / "small.bim").read_bytes()
        finally:
            self._terminate(proc, mnt)
        assert log_path.exists()
        lines = log_path.read_text().splitlines()
        assert len(lines) > 0
        paths = {line.split('"path":')[1].split('"')[1] for line in lines}
        assert "small.bed" in paths

    def test_log_level_accepted(self, tmp_path, fx_small_vcz):
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "biofuse.cli",
                "mount-plink",
                str(fx_small_vcz.path),
                str(mnt),
                "--log-level",
                "info",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            self._wait_for_mount(mnt, proc, timeout=15)
            assert (mnt / "small.bed").stat().st_size > 0
        finally:
            self._terminate(proc, mnt)

    def test_samples_filter(self, tmp_path, fx_small_vcz):
        """--samples reaches the worker subprocess and shrinks .fam."""
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "biofuse.cli",
                "mount-plink",
                str(fx_small_vcz.path),
                str(mnt),
                "--samples",
                "tsk_0,tsk_1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            self._wait_for_mount(mnt, proc, timeout=15)
            fam_text = (mnt / "small.fam").read_text()
            assert fam_text.count("\n") == 2
            assert "tsk_0" in fam_text
            assert "tsk_1" in fam_text
            assert "tsk_2" not in fam_text
        finally:
            self._terminate(proc, mnt)

    def test_bgen_mounts_and_serves_files(self, tmp_path, fx_small_vcz):
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "biofuse.cli",
                "mount-bgen",
                str(fx_small_vcz.path),
                str(mnt),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            self._wait_for_mount(mnt, proc, timeout=15)
            names = sorted(p.name for p in mnt.iterdir())
            assert "small.bgen" in names
            assert "small.sample" in names
            assert "small.bgen.bgi" in names
            assert (mnt / "small.bgen").stat().st_size > 0
            # Two header rows + one row per sample.
            sample_lines = (mnt / "small.sample").read_text().splitlines()
            assert len(sample_lines) == 2 + fx_small_vcz.num_samples
        finally:
            self._terminate(proc, mnt)

    def test_plink_no_bim_suppresses_sidecar(self, tmp_path, fx_small_vcz):
        """``--no-bim`` removes the ``.bim`` sidecar from the mount."""
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "biofuse.cli",
                "mount-plink",
                str(fx_small_vcz.path),
                str(mnt),
                "--no-bim",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            self._wait_for_mount(mnt, proc, timeout=15)
            names = sorted(p.name for p in mnt.iterdir())
            assert names == ["small.bed", "small.fam"]
        finally:
            self._terminate(proc, mnt)

    def test_bgen_no_bgi_suppresses_sidecar(self, tmp_path, fx_small_vcz):
        """``--no-bgi`` removes the ``.bgen.bgi`` sidecar from the mount."""
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "biofuse.cli",
                "mount-bgen",
                str(fx_small_vcz.path),
                str(mnt),
                "--no-bgi",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            self._wait_for_mount(mnt, proc, timeout=15)
            names = sorted(p.name for p in mnt.iterdir())
            assert names == ["small.bgen", "small.sample"]
        finally:
            self._terminate(proc, mnt)

    def test_bgen_basename_override(self, tmp_path, fx_small_vcz):
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "biofuse.cli",
                "mount-bgen",
                str(fx_small_vcz.path),
                str(mnt),
                "--basename",
                "custom",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            self._wait_for_mount(mnt, proc, timeout=15)
            assert sorted(p.name for p in mnt.iterdir()) == [
                "custom.bgen",
                "custom.bgen.bgi",
                "custom.sample",
            ]
        finally:
            self._terminate(proc, mnt)

    def test_basename_override(self, tmp_path, fx_small_vcz):
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "biofuse.cli",
                "mount-plink",
                str(fx_small_vcz.path),
                str(mnt),
                "--basename",
                "custom",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            self._wait_for_mount(mnt, proc, timeout=15)
            assert sorted(p.name for p in mnt.iterdir()) == [
                "custom.bed",
                "custom.bim",
                "custom.fam",
            ]
        finally:
            self._terminate(proc, mnt)

    def test_max_alleles_filter_admits_multiallelic(
        self, tmp_path, fx_multiallelic_vcz
    ):
        """``--max-alleles 2`` (the workaround the error message
        recommends) flows through to vcztools' reader and drops the
        multi-allelic sites before the BedEncoder sees them, so the
        mount succeeds with a smaller variant set."""
        # Sanity: the fixture really does contain multi-allelic sites,
        # otherwise the test couldn't prove the filter ran.
        assert fx_multiallelic_vcz.num_biallelic_sites < (
            fx_multiallelic_vcz.num_variants
        )
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "biofuse.cli",
                "mount-plink",
                str(fx_multiallelic_vcz.path),
                str(mnt),
                "--max-alleles",
                "2",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            self._wait_for_mount(mnt, proc, timeout=30)
            bim_text = (mnt / "multiallelic.bim").read_text()
            bim_lines = bim_text.splitlines()
            assert len(bim_lines) == fx_multiallelic_vcz.num_biallelic_sites
            fam_lines = (mnt / "multiallelic.fam").read_text().splitlines()
            assert len(fam_lines) == fx_multiallelic_vcz.num_samples
            bytes_per_variant = (fx_multiallelic_vcz.num_samples + 3) // 4
            expected_bed_size = (
                3 + fx_multiallelic_vcz.num_biallelic_sites * bytes_per_variant
            )
            assert (mnt / "multiallelic.bed").stat().st_size == expected_bed_size
        finally:
            self._terminate(proc, mnt)

    def test_multiallelic_vcz_clean_error(self, tmp_path, fx_multiallelic_vcz):
        """A multi-allelic VCZ must surface as a single clean error line
        with no Python traceback. The plink-server logs the helpful cause
        ('Multi-allelic ...') at ERROR; the parent emits a Click
        'Error: ...' line and exits non-zero."""
        mnt = tmp_path / "mnt"
        mnt.mkdir()
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "biofuse.cli",
                "mount-plink",
                str(fx_multiallelic_vcz.path),
                str(mnt),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            stdout, stderr = proc.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise
        stderr_text = stderr.decode()
        assert proc.returncode != 0
        assert "Traceback (most recent call last)" not in stderr_text
        assert "Multi-allelic" in stderr_text
        assert "Error:" in stderr_text
        assert not os.path.ismount(mnt)

    @staticmethod
    def _wait_for_mount(mnt: pathlib.Path, proc, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if os.path.ismount(mnt):
                return
            if proc.poll() is not None:
                stderr = proc.stderr.read().decode() if proc.stderr else ""
                raise RuntimeError(
                    f"biofuse exited prematurely: {proc.returncode} stderr={stderr!r}"
                )
            time.sleep(0.05)
        raise RuntimeError(f"mountpoint {mnt} not live within {timeout}s")

    @staticmethod
    def _terminate(proc, mnt: pathlib.Path) -> None:
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            subprocess.run(
                ["fusermount3", "-z", "-u", str(mnt)],
                capture_output=True,
                check=False,
            )
            proc.kill()
            proc.wait(timeout=5)
