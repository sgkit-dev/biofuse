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

    def test_mount_plink_help(self):
        result = CliRunner().invoke(cli.biofuse_main, ["mount-plink", "--help"])
        assert result.exit_code == 0
        assert "VCZ_URL" in result.output
        assert "MOUNT_DIR" in result.output

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
