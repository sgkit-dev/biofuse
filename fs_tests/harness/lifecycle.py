"""Mount / unmount cycling stress.

Spawn the real biofuse CLI, wait for the mount, do a smoke read of every
file, signal teardown, wait for unmount — N times. The runner verifies
that every cycle completes within a generous timeout, no orphaned mounts
remain, and per-cycle wall time stays bounded.
"""

import logging
import pathlib
import statistics
import subprocess
import time

from . import fixtures, tools
from . import mount as mount_mod

logger = logging.getLogger(__name__)


def _findmnt_count(mountpoint: pathlib.Path) -> int:
    """Count active fuse.biofuse mounts at ``mountpoint`` (0 if none)."""
    if not tools.have_tool("findmnt"):
        return 0
    proc = subprocess.run(
        ["findmnt", "--noheadings", "--types", "fuse.biofuse", str(mountpoint)],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode == 0 and proc.stdout.strip():
        return 1
    return 0


def run(
    *,
    log_dir: pathlib.Path,
    iterations: int = 50,
    per_cycle_budget_s: float = 30.0,
) -> tools.RunnerResult:
    started = time.monotonic()
    log_dir.mkdir(parents=True, exist_ok=True)
    spec = fixtures.SMALL
    vcz_path = fixtures.get_or_build(spec)
    mountpoint = log_dir / "mnt"

    logger.info(
        "lifecycle: %d mount/unmount cycles (per-cycle budget %.0fs)",
        iterations,
        per_cycle_budget_s,
    )
    cycle_times: list[float] = []
    failures = 0
    last_error: str | None = None
    progress_every = max(1, iterations // 10)

    for i in range(iterations):
        cycle_start = time.monotonic()
        try:
            with mount_mod.BiofuseMount(
                str(vcz_path),
                mountpoint,
                log_path=log_dir / f"cycle-{i:03d}.log",
                startup_timeout_s=per_cycle_budget_s,
            ) as mnt:
                for suffix in (".bed", ".bim", ".fam"):
                    p = mnt / f"{spec.name}{suffix}"
                    p.read_bytes()
        except Exception as exc:
            failures += 1
            last_error = f"cycle {i}: {type(exc).__name__}: {exc}"
            logger.error(last_error)
        cycle_times.append(time.monotonic() - cycle_start)
        cycle_num = i + 1
        if cycle_num % progress_every == 0 or cycle_num == iterations:
            running_mean = statistics.mean(cycle_times)
            logger.info(
                "lifecycle: cycle %d/%d (mean=%.2fs, failures=%d)",
                cycle_num,
                iterations,
                running_mean,
                failures,
            )

    orphan_count = _findmnt_count(mountpoint)
    if cycle_times:
        mean = statistics.mean(cycle_times)
        p99 = (
            statistics.quantiles(cycle_times, n=100)[98]
            if len(cycle_times) >= 100
            else max(cycle_times)
        )
        max_t = max(cycle_times)
    else:
        mean = p99 = max_t = 0.0

    checks = [
        tools.CheckResult(
            name="lifecycle:cycles_complete",
            passed=failures == 0,
            duration_s=sum(cycle_times),
            detail=(
                f"completed {iterations - failures}/{iterations}; "
                f"mean={mean:.2f}s p99={p99:.2f}s max={max_t:.2f}s"
                + (f"; last error: {last_error}" if last_error else "")
            ),
        ),
        tools.CheckResult(
            name="lifecycle:no_orphan_mounts",
            passed=orphan_count == 0,
            duration_s=0.0,
            detail=f"orphan fuse.biofuse mounts at {mountpoint}: {orphan_count}",
        ),
        tools.CheckResult(
            name="lifecycle:max_cycle_within_budget",
            passed=max_t <= per_cycle_budget_s,
            duration_s=0.0,
            detail=f"max cycle {max_t:.2f}s vs budget {per_cycle_budget_s:.1f}s",
        ),
    ]

    duration = time.monotonic() - started
    logger.info(
        "lifecycle: %d/%d cycles complete; mean=%.2fs p99=%.2fs max=%.2fs",
        iterations - failures,
        iterations,
        mean,
        p99,
        max_t,
    )
    return tools.RunnerResult(
        runner="lifecycle",
        passed=all(c.passed for c in checks),
        duration_s=duration,
        checks=checks,
        summary=f"{iterations} mount/unmount cycles; mean {mean:.2f}s",
    )
