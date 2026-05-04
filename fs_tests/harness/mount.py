"""Spawn a real ``biofuse`` mount in a subprocess and tear it down cleanly."""

import logging
import os
import pathlib
import shutil
import signal
import subprocess
import time

logger = logging.getLogger(__name__)


def wait_for_mount(mountpoint: pathlib.Path, timeout: float = 15.0) -> None:
    """Poll until ``mountpoint`` is a live mount, or raise on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.ismount(mountpoint):
            return
        time.sleep(0.05)
    raise RuntimeError(f"mountpoint {mountpoint} not live after {timeout:.1f}s")


def wait_for_unmount(mountpoint: pathlib.Path, timeout: float = 15.0) -> None:
    """Poll until ``mountpoint`` is no longer mounted."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not os.path.ismount(mountpoint):
            return
        time.sleep(0.05)
    raise RuntimeError(f"mountpoint {mountpoint} still mounted after {timeout:.1f}s")


def force_unmount(mountpoint: pathlib.Path) -> None:
    """Run ``fusermount3 -z -u`` on ``mountpoint``, swallowing failures."""
    fusermount = shutil.which("fusermount3") or shutil.which("fusermount")
    if fusermount is None:
        logger.warning("no fusermount executable found; cannot unmount %s", mountpoint)
        return
    subprocess.run(
        [fusermount, "-z", "-u", str(mountpoint)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
        timeout=10,
    )


class BiofuseMount:
    """Context manager that runs ``biofuse mount-plink`` in a subprocess.

    Spawns the real CLI so the harness exercises the same code path a user
    hits. Each mount is its own subprocess, sidestepping pyfuse3's
    one-mount-per-process limitation entirely.
    """

    def __init__(
        self,
        vcz_url: str,
        mountpoint: pathlib.Path,
        *,
        basename: str | None = None,
        log_path: pathlib.Path | None = None,
        startup_timeout_s: float = 30.0,
        shutdown_timeout_s: float = 15.0,
    ) -> None:
        self.vcz_url = vcz_url
        self.mountpoint = mountpoint
        self.basename = basename
        self.log_path = log_path
        self.startup_timeout_s = startup_timeout_s
        self.shutdown_timeout_s = shutdown_timeout_s
        self._proc: subprocess.Popen[bytes] | None = None
        self._log_handle = None

    def _build_cmd(self) -> list[str]:
        biofuse_bin = shutil.which("biofuse")
        if biofuse_bin is None:
            raise RuntimeError(
                "biofuse executable not found on PATH; "
                "run the harness via fs_tests/run.sh which uses uv run"
            )
        cmd: list[str] = [
            biofuse_bin,
            "mount-plink",
            self.vcz_url,
            str(self.mountpoint),
        ]
        if self.basename is not None:
            cmd += ["--basename", self.basename]
        return cmd

    def __enter__(self) -> pathlib.Path:
        self.mountpoint.mkdir(parents=True, exist_ok=True)
        cmd = self._build_cmd()
        log_target: int | object = subprocess.DEVNULL
        if self.log_path is not None:
            self._log_handle = open(self.log_path, "wb")  # noqa: SIM115
            log_target = self._log_handle
        logger.debug("spawning %s", " ".join(cmd))
        self._proc = subprocess.Popen(
            cmd,
            stdout=log_target,
            stderr=subprocess.STDOUT,
            close_fds=True,
        )
        try:
            wait_for_mount(self.mountpoint, self.startup_timeout_s)
        except Exception:
            self._terminate()
            raise
        logger.info("mount live at %s", self.mountpoint)
        return self.mountpoint

    def __exit__(self, exc_type, exc, tb) -> None:
        self._terminate()

    def _terminate(self) -> None:
        proc = self._proc
        if proc is None:
            return
        logger.debug("tearing down mount at %s", self.mountpoint)
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=self.shutdown_timeout_s)
            except subprocess.TimeoutExpired:
                logger.warning("mount subprocess did not exit on SIGTERM; SIGKILL")
                proc.kill()
                proc.wait(timeout=5)
        force_unmount(self.mountpoint)
        try:
            wait_for_unmount(self.mountpoint, timeout=5.0)
        except RuntimeError as exc:
            logger.warning("%s", exc)
        if self._log_handle is not None:
            self._log_handle.close()
            self._log_handle = None
        self._proc = None
        logger.debug("mount %s torn down", self.mountpoint)
