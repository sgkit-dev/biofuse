"""Liveness probes against a biofuse mount while it's under heavy IO load.

Spawns a background fio stressor against the streaming ``.bed`` file, then
loops foreground probes that exercise the rest of the mount (``readdir`` plus
small reads of the cached static files). The goal is to detect cases where
heavy load on one fh starves out unrelated mount operations.

The streaming file is *not* probed: ``fio-multithread.fio`` is expected to
saturate it, and this runner exists to confirm the other entry points stay
responsive under that pressure.
"""

import concurrent.futures as cf
import dataclasses
import logging
import os
import pathlib
import signal
import subprocess
import time

from . import fio_runner, fixtures, tools
from . import mount as mount_mod

logger = logging.getLogger(__name__)

DEFAULT_DURATION_S = 30.0
DEFAULT_PROBE_INTERVAL_S = 0.5
DEFAULT_PROBE_TIMEOUT_S = 5.0
BACKGROUND_JOB = "fio-multithread.fio"


@dataclasses.dataclass
class _ProbeStats:
    name: str
    attempts: int = 0
    successes: int = 0
    timeouts: int = 0
    errors: int = 0
    max_latency_ms: float = 0.0
    first_error: str = ""


def _probe_readdir(mountpoint: pathlib.Path) -> None:
    entries = os.listdir(mountpoint)
    if len(entries) == 0:
        raise RuntimeError(f"mount {mountpoint} returned empty listing")


def _probe_read_static(path: pathlib.Path) -> None:
    with open(path, "rb") as fh:
        data = fh.read(4096)
    if len(data) == 0:
        raise RuntimeError(f"static file {path} returned 0 bytes")


def _run_bounded(
    executor: cf.ThreadPoolExecutor,
    fn,
    timeout_s: float,
) -> None:
    """Run ``fn`` in ``executor`` and raise TimeoutError if it doesn't finish.

    A hung syscall keeps occupying the worker thread; the executor is sized
    so a small backlog of stuck probes doesn't starve subsequent attempts,
    and the runner's mount teardown unblocks any threads still parked when
    the run ends.
    """
    future = executor.submit(fn)
    try:
        future.result(timeout=timeout_s)
    except cf.TimeoutError as exc:
        raise TimeoutError(f"probe exceeded {timeout_s}s") from exc


def _spawn_background_fio(
    target_file: pathlib.Path,
    log_dir: pathlib.Path,
    duration_s: float,
) -> tuple[subprocess.Popen, object]:
    job_path = fio_runner.JOBS_DIR / BACKGROUND_JOB
    env = os.environ.copy()
    env["TARGET_FILE"] = str(target_file)
    output_json = log_dir / "background-fio.json"
    log_path = log_dir / "background-fio.log"
    cmd = [
        "fio",
        "--output-format=json",
        f"--output={output_json}",
        f"--runtime={int(duration_s)}",
        "--time_based",
        str(job_path),
    ]
    log_handle = open(log_path, "wb")  # noqa: SIM115 — closed in caller
    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        close_fds=True,
    )
    return proc, log_handle


def _terminate_background(proc: subprocess.Popen, log_handle) -> None:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    log_handle.close()


def _build_probes(mnt: pathlib.Path, fixture_name: str) -> dict[str, tuple]:
    return {
        "readdir": (_probe_readdir, mnt),
        "static-read:.bim": (_probe_read_static, mnt / f"{fixture_name}.bim"),
        "static-read:.fam": (_probe_read_static, mnt / f"{fixture_name}.fam"),
    }


def run(
    *,
    log_dir: pathlib.Path,
    duration_s: float = DEFAULT_DURATION_S,
    probe_interval_s: float = DEFAULT_PROBE_INTERVAL_S,
    probe_timeout_s: float = DEFAULT_PROBE_TIMEOUT_S,
) -> tools.RunnerResult:
    """Run a background fio stressor and verify mount liveness probes."""
    started = time.monotonic()
    if not tools.have_tool("fio"):
        return tools.RunnerResult(
            runner="active-under-stress",
            passed=True,
            duration_s=0.0,
            skipped=True,
            skip_reason="fio not installed",
        )
    log_dir.mkdir(parents=True, exist_ok=True)
    spec = fixtures.MEDIUM
    vcz_path = fixtures.get_or_build(spec)

    mountpoint = log_dir / "mnt"
    checks: list[tools.CheckResult] = []
    with mount_mod.BiofuseMount(
        str(vcz_path),
        mountpoint,
        log_path=log_dir / "mount.log",
        access_log_path=log_dir / "access.jsonl",
    ) as mnt:
        bed_path = mnt / f"{spec.name}.bed"
        probes = _build_probes(mnt, spec.name)
        stats: dict[str, _ProbeStats] = {
            name: _ProbeStats(name=name) for name in probes
        }

        bg_proc, bg_log = _spawn_background_fio(bed_path, log_dir, duration_s)
        logger.info(
            "active-under-stress: background %s pid=%d, runtime=%.0fs",
            BACKGROUND_JOB,
            bg_proc.pid,
            duration_s,
        )

        executor = cf.ThreadPoolExecutor(max_workers=max(8, 2 * len(probes)))
        try:
            deadline = time.monotonic() + duration_s
            while time.monotonic() < deadline:
                for name, (fn, arg) in probes.items():
                    s = stats[name]
                    s.attempts += 1
                    t0 = time.monotonic()
                    try:
                        _run_bounded(
                            executor,
                            lambda f=fn, a=arg: f(a),
                            probe_timeout_s,
                        )
                        latency_ms = (time.monotonic() - t0) * 1000
                        if latency_ms > s.max_latency_ms:
                            s.max_latency_ms = latency_ms
                        s.successes += 1
                    except TimeoutError as exc:
                        s.timeouts += 1
                        if s.first_error == "":
                            s.first_error = str(exc)
                        logger.warning("probe %s: %s", name, exc)
                    except Exception as exc:
                        s.errors += 1
                        if s.first_error == "":
                            s.first_error = f"{type(exc).__name__}: {exc}"
                        logger.warning("probe %s failed: %s", name, exc)
                time.sleep(probe_interval_s)
        finally:
            _terminate_background(bg_proc, bg_log)
            executor.shutdown(wait=False)

        for name, s in stats.items():
            passed = s.attempts > 0 and s.timeouts == 0 and s.errors == 0
            detail_parts = [
                f"attempts={s.attempts}",
                f"ok={s.successes}",
                f"timeouts={s.timeouts}",
                f"errors={s.errors}",
                f"max_latency={s.max_latency_ms:.1f}ms",
            ]
            if s.first_error:
                detail_parts.append(f"first={s.first_error!r}")
            checks.append(
                tools.CheckResult(
                    name=f"liveness:{name}",
                    passed=passed,
                    duration_s=0.0,
                    detail=" ".join(detail_parts),
                )
            )

    duration = time.monotonic() - started
    return tools.RunnerResult(
        runner="active-under-stress",
        passed=all(c.passed for c in checks),
        duration_s=duration,
        checks=checks,
        summary=(
            f"background={BACKGROUND_JOB} duration={duration_s:.0f}s "
            f"probes={len(checks)}"
        ),
    )
