"""fio-driven read-pattern stress against a live biofuse mount."""

import dataclasses
import json
import logging
import os
import pathlib
import subprocess
import time

from . import fixtures, tools
from . import mount as mount_mod

logger = logging.getLogger(__name__)

JOBS_DIR = pathlib.Path(__file__).resolve().parent.parent / "jobs"

JOB_FILES = [
    "fio-seq-read.fio",
    "fio-rand-read.fio",
    "fio-mmap-read.fio",
    "fio-multithread.fio",
]

# Jobs we expect to drive concurrent FUSE reads. ``mmap`` + kernel
# readahead drives parallel page-fault reads on a single fh;
# ``multithread`` opens one fh per fio process and reads all of them
# simultaneously.
CONCURRENT_JOBS: dict[str, str] = {
    "fio-mmap-read.fio": "single-fh",
    "fio-multithread.fio": "multi-fh",
}


@dataclasses.dataclass
class _FioOutcome:
    job_file: str
    errors: int
    total_io_bytes: int
    runtime_ms: int
    raw_json: dict


@dataclasses.dataclass
class _ConcurrencyStats:
    max_overlap: int
    distinct_fhs: int
    record_count: int


def _run_fio_job(
    job_path: pathlib.Path,
    target_file: pathlib.Path,
    log_dir: pathlib.Path,
    runtime_override_s: int | None,
) -> _FioOutcome:
    env = os.environ.copy()
    env["TARGET_FILE"] = str(target_file)
    output_json = log_dir / f"{job_path.stem}.json"
    cmd = [
        "fio",
        "--output-format=json",
        f"--output={output_json}",
    ]
    if runtime_override_s is not None:
        cmd += [f"--runtime={runtime_override_s}", "--time_based"]
    cmd += [str(job_path)]
    logger.debug("fio cmd: %s", " ".join(cmd))
    proc = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    log_target = log_dir / f"{job_path.stem}.log"
    log_target.write_text(
        f"$ {' '.join(cmd)}\n"
        f"--- stdout ---\n{proc.stdout}\n"
        f"--- stderr ---\n{proc.stderr}\n"
        f"--- returncode: {proc.returncode}\n"
    )
    if proc.returncode != 0 or not output_json.exists():
        return _FioOutcome(
            job_file=job_path.name,
            errors=1,
            total_io_bytes=0,
            runtime_ms=0,
            raw_json={"returncode": proc.returncode, "stderr": proc.stderr},
        )
    raw = json.loads(output_json.read_text())
    total_errors = 0
    total_bytes = 0
    runtime_ms = 0
    for job in raw.get("jobs", []):
        total_errors += int(job.get("error", 0))
        for direction in ("read", "write"):
            section = job.get(direction, {})
            total_bytes += int(section.get("io_bytes", 0))
            runtime_ms = max(runtime_ms, int(section.get("runtime", 0)))
    return _FioOutcome(
        job_file=job_path.name,
        errors=total_errors,
        total_io_bytes=total_bytes,
        runtime_ms=runtime_ms,
        raw_json=raw,
    )


def _slice_access_log(
    access_log_path: pathlib.Path, t_lo: float, t_hi: float
) -> list[dict]:
    """Return access-log records whose ``t_start`` falls in ``[t_lo, t_hi]``.

    Robust to the file not yet existing (e.g. job timed out before any
    reads were served) and to malformed trailing lines (e.g. a partial
    line at end-of-file if the mount was killed mid-write).
    """
    if not access_log_path.exists():
        return []
    records: list[dict] = []
    with access_log_path.open() as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line == "":
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            t_start = rec.get("t_start")
            if t_start is None or not (t_lo <= t_start <= t_hi):
                continue
            records.append(rec)
    return records


def _concurrency_stats(records: list[dict]) -> _ConcurrencyStats:
    """Compute max-overlap depth and number of distinct fhs."""
    events: list[tuple[float, int]] = []
    fhs: set[int] = set()
    for r in records:
        try:
            t_start = float(r["t_start"])
            t_end = float(r["t_end"])
        except (KeyError, TypeError, ValueError):
            continue
        events.append((t_start, +1))
        # Use t_end + epsilon at same priority? Sweep-line convention:
        # process ``-1`` events before ``+1`` at the same instant so
        # zero-duration reads don't show overlap with their neighbours.
        events.append((t_end, -1))
        fhs.add(int(r.get("fh", -1)))
    events.sort(key=lambda e: (e[0], e[1]))
    depth = 0
    max_depth = 0
    for _, delta in events:
        depth += delta
        if depth > max_depth:
            max_depth = depth
    return _ConcurrencyStats(
        max_overlap=max_depth, distinct_fhs=len(fhs), record_count=len(records)
    )


def run(
    *,
    use_large_fixture: bool = False,
    log_dir: pathlib.Path,
    runtime_override_s: int | None = None,
) -> tools.RunnerResult:
    """Mount biofuse with a real plink fileset and run all fio jobs."""
    started = time.monotonic()
    if not tools.have_tool("fio"):
        logger.info("fio: SKIP (fio not installed)")
        return tools.RunnerResult(
            runner="fio",
            passed=True,  # skipped does not count as failure
            duration_s=0.0,
            skipped=True,
            skip_reason="fio not installed",
        )
    log_dir.mkdir(parents=True, exist_ok=True)
    spec = fixtures.LARGE if use_large_fixture else fixtures.MEDIUM
    vcz_path = fixtures.get_or_build(spec)

    mountpoint = log_dir / "mnt"
    access_log_path = log_dir / "access.jsonl"
    checks: list[tools.CheckResult] = []
    with mount_mod.BiofuseMount(
        str(vcz_path),
        mountpoint,
        log_path=log_dir / "mount.log",
        access_log_path=access_log_path,
    ) as mnt:
        target_file = mnt / f"{spec.name}.bed"
        if not target_file.exists():
            return tools.RunnerResult(
                runner="fio",
                passed=False,
                duration_s=time.monotonic() - started,
                summary=f"target file missing: {target_file}",
            )
        fsize_mb = target_file.stat().st_size / 1024 / 1024
        logger.info(
            "fio: target=%s (%.1f MB), %d jobs",
            target_file,
            fsize_mb,
            len(JOB_FILES),
        )
        for job_name in JOB_FILES:
            job_path = JOBS_DIR / job_name
            logger.info("fio: running job %s", job_name)
            t0 = time.monotonic()
            timed_out = False
            try:
                outcome = _run_fio_job(
                    job_path, target_file, log_dir, runtime_override_s
                )
            except subprocess.TimeoutExpired as exc:
                logger.warning("fio: job %s timed out: %s", job_name, exc)
                timed_out = True
                outcome = None
                detail = f"timeout: {exc}"
            t1 = time.monotonic()
            duration = t1 - t0
            if timed_out:
                checks.append(
                    tools.CheckResult(
                        name=f"fio:{job_name}",
                        passed=False,
                        duration_s=duration,
                        detail=detail,
                    )
                )
            else:
                mb = outcome.total_io_bytes / 1024 / 1024
                mbs = (
                    f"{(mb * 1000 / outcome.runtime_ms):.1f} MB/s"
                    if outcome.runtime_ms > 0
                    else "?"
                )
                detail = (
                    f"errors={outcome.errors} io={mb:.1f} MB "
                    f"runtime={outcome.runtime_ms}ms throughput={mbs}"
                )
                logger.info("fio %s: %s", job_name, detail)
                checks.append(
                    tools.CheckResult(
                        name=f"fio:{job_name}",
                        passed=outcome.errors == 0,
                        duration_s=duration,
                        detail=detail,
                    )
                )

            kind = CONCURRENT_JOBS.get(job_name)
            if kind is not None:
                records = _slice_access_log(access_log_path, t0, t1)
                stats = _concurrency_stats(records)
                conc_detail = (
                    f"records={stats.record_count} fhs={stats.distinct_fhs} "
                    f"max_overlap={stats.max_overlap}"
                )
                # Required signature: more than one read in flight at
                # some instant. ``multi-fh`` jobs additionally require
                # at least 2 distinct fhs in the trace.
                passed = stats.max_overlap >= 2
                if kind == "multi-fh":
                    passed = passed and stats.distinct_fhs >= 2
                logger.info("fio %s concurrency: %s", job_name, conc_detail)
                checks.append(
                    tools.CheckResult(
                        name=f"fio:{job_name}:concurrent",
                        passed=passed,
                        duration_s=0.0,
                        detail=conc_detail,
                    )
                )

    duration = time.monotonic() - started
    return tools.RunnerResult(
        runner="fio",
        passed=all(c.passed for c in checks),
        duration_s=duration,
        checks=checks,
        summary=f"fixture={spec.name} target=*.bed",
    )
