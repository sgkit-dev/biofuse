"""Read-stress against a live biofuse mount.

stress-ng's filesystem stressors (--readahead, --seek, --io) create their
own working files in --temp-path and therefore cannot run against a
read-only mount. The runner instead uses stress-ng (when available) as a
background CPU/memory load and runs an in-harness multi-process open/read
loop against the mount to verify the filesystem stays responsive under
system pressure.
"""

import logging
import multiprocessing
import os
import pathlib
import re
import subprocess
import time

from harness import fixtures, tools
from harness import mount as mount_mod

logger = logging.getLogger(__name__)


def _stress_ng_metric(stderr: str, metric: str) -> int | None:
    pattern = rf"\b{re.escape(metric)}\s+([0-9]+)\b"
    m = re.search(pattern, stderr)
    return int(m.group(1)) if m else None


def _open_loop_worker(target: str, duration_s: float, result_q) -> None:
    deadline = time.monotonic() + duration_s
    ops = 0
    errs = 0
    while time.monotonic() < deadline:
        try:
            fd = os.open(target, os.O_RDONLY)
            try:
                # Read the full file in 64K chunks to mimic real consumers.
                while True:
                    if not os.read(fd, 65536):
                        break
            finally:
                os.close(fd)
            ops += 1
        except OSError:
            errs += 1
    result_q.put((ops, errs))


def _run_open_loop(
    target_file: pathlib.Path,
    *,
    n_workers: int,
    duration_s: float,
    name: str,
) -> tools.CheckResult:
    logger.info(
        "open-loop: workers=%d, duration=%.0fs, target=%s",
        n_workers,
        duration_s,
        target_file,
    )
    started = time.monotonic()
    ctx = multiprocessing.get_context("fork")
    q: multiprocessing.Queue = ctx.Queue()
    workers = [
        ctx.Process(
            target=_open_loop_worker,
            args=(str(target_file), duration_s, q),
        )
        for _ in range(n_workers)
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join(timeout=duration_s + 30)
    total_ops = 0
    total_errs = 0
    while not q.empty():
        ops, errs = q.get_nowait()
        total_ops += ops
        total_errs += errs
    duration = time.monotonic() - started
    logger.info(
        "open-loop done: workers=%d ops=%d errors=%d (%.1fs)",
        n_workers,
        total_ops,
        total_errs,
        duration,
    )
    return tools.CheckResult(
        name=name,
        passed=total_errs == 0,
        duration_s=duration,
        detail=f"workers={n_workers} ops={total_ops} errors={total_errs}",
    )


def _start_background_load(duration_s: float) -> subprocess.Popen | None:
    """Start stress-ng CPU+VM background load if stress-ng is present."""
    if not tools.have_tool("stress-ng"):
        logger.info("stress-ng background load: SKIP (stress-ng not installed)")
        return None
    cpus = max(2, (os.cpu_count() or 4) // 2)
    cmd = [
        "stress-ng",
        "--cpu",
        str(cpus),
        "--vm",
        "2",
        "--vm-bytes",
        "256M",
        "-t",
        str(int(duration_s)),
        "--metrics-brief",
    ]
    logger.info(
        "stress-ng background load: cpu=%d vm=2 vm-bytes=256M duration=%.0fs",
        cpus,
        duration_s,
    )
    logger.debug("stress-ng cmd: %s", " ".join(cmd))
    return subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


def _stop_background_load(
    proc: subprocess.Popen | None,
    log_dir: pathlib.Path,
) -> tools.CheckResult | None:
    if proc is None:
        return tools.CheckResult(
            name="stress-ng:background-load",
            passed=True,
            duration_s=0.0,
            detail="skipped: stress-ng not installed",
        )
    try:
        _, stderr = proc.communicate(timeout=120)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate(timeout=10)
        return tools.CheckResult(
            name="stress-ng:background-load",
            passed=False,
            duration_s=0.0,
            detail="background stress-ng did not exit",
        )
    stderr_text = stderr.decode("utf-8", errors="replace") if stderr else ""
    (log_dir / "stress-ng-background.log").write_text(stderr_text)
    failed = _stress_ng_metric(stderr_text, "failed")
    completed = _stress_ng_metric(stderr_text, "completed")
    detail = f"rc={proc.returncode} failed={failed} completed={completed}"
    logger.info("stress-ng background load done: %s", detail)
    passed = proc.returncode == 0 and (failed is None or failed == 0)
    return tools.CheckResult(
        name="stress-ng:background-load",
        passed=passed,
        duration_s=0.0,
        detail=detail,
    )


def run(*, log_dir: pathlib.Path, duration_s: float = 30.0) -> tools.RunnerResult:
    """Hammer the mount with parallel readers, optionally under stress-ng load."""
    started = time.monotonic()
    log_dir.mkdir(parents=True, exist_ok=True)
    spec = fixtures.MEDIUM
    vcz_path = fixtures.get_or_build(spec)
    mountpoint = log_dir / "mnt"

    checks: list[tools.CheckResult] = []
    with mount_mod.BiofuseMount(
        str(vcz_path), mountpoint, log_path=log_dir / "mount.log"
    ) as mnt:
        target_file = mnt / f"{spec.name}.bed"
        if not target_file.exists():
            return tools.RunnerResult(
                runner="stress-ng",
                passed=False,
                duration_s=time.monotonic() - started,
                summary=f"target file missing: {target_file}",
            )

        bg_proc = _start_background_load(duration_s)
        try:
            checks.append(
                _run_open_loop(
                    target_file,
                    n_workers=4,
                    duration_s=duration_s,
                    name="open-loop:4p:30s",
                )
            )
            checks.append(
                _run_open_loop(
                    target_file,
                    n_workers=16,
                    duration_s=duration_s,
                    name="open-loop:16p:30s",
                )
            )
        finally:
            bg_check = _stop_background_load(bg_proc, log_dir)
            if bg_check is not None:
                checks.append(bg_check)

    duration = time.monotonic() - started
    return tools.RunnerResult(
        runner="stress-ng",
        passed=all(c.passed for c in checks),
        duration_s=duration,
        checks=checks,
        summary="open/read loops + optional stress-ng background load",
    )
