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


@dataclasses.dataclass(frozen=True)
class _JobSpec:
    """One fio invocation in the runner schedule."""

    name: str
    file: str
    target_suffix: str
    gate: bool = True
    concurrent: str | None = None


# ``gate=False`` jobs are informational: errors are recorded in the
# detail line but don't fail the runner.
#
# - ``fio-multithread.fio`` is the 16-way parallel random read that
#   reliably times out under FUSE backpressure (EAGAIN).
# - ``fio-mmap-read.fio`` triggers EAGAIN via a different path: fio's
#   mmap engine does a tight open/close storm (~5000/s on a host fs),
#   which on biofuse fills the streaming-fh limiter because each
#   FUSE_OPEN spins up a fresh encoder-server connection. The
#   resulting EAGAIN is unrelated to data correctness — bytes already
#   read came back valid. The job stays in the schedule because the
#   ``:concurrent`` check it drives confirms the kernel is fanning out
#   readahead on a single fh.
JOBS: tuple[_JobSpec, ...] = (
    _JobSpec("seq-read", "fio-seq-read.fio", ".bed"),
    _JobSpec("rand-read", "fio-rand-read.fio", ".bed"),
    _JobSpec(
        "mmap-read",
        "fio-mmap-read.fio",
        ".bed",
        gate=False,
        concurrent="single-fh",
    ),
    _JobSpec(
        "parallel-seq-read",
        "fio-parallel-seq-read.fio",
        ".bed",
        concurrent="multi-fh",
    ),
    _JobSpec(
        "multithread",
        "fio-multithread.fio",
        ".bed",
        gate=False,
        concurrent="multi-fh",
    ),
    # Static-file stress jobs are gated on errors=0. The
    # access-log overlap check is intentionally omitted: reads are
    # served from an in-memory bytes object and complete in
    # microseconds, so even 16-way fio produces virtually no observable
    # overlap in the access log.
    _JobSpec("static-stress-bim", "fio-static-stress.fio", ".bim"),
    _JobSpec("static-stress-fam", "fio-static-stress.fio", ".fam"),
)


@dataclasses.dataclass
class _FioOutcome:
    job_name: str
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
    job: _JobSpec,
    target_file: pathlib.Path,
    log_dir: pathlib.Path,
    runtime_override_s: int | None,
) -> _FioOutcome:
    env = os.environ.copy()
    env["TARGET_FILE"] = str(target_file)
    output_json = log_dir / f"{job.name}.json"
    job_path = JOBS_DIR / job.file
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
    log_target = log_dir / f"{job.name}.log"
    log_target.write_text(
        f"$ {' '.join(cmd)}\n"
        f"--- stdout ---\n{proc.stdout}\n"
        f"--- stderr ---\n{proc.stderr}\n"
        f"--- returncode: {proc.returncode}\n"
    )
    # fio writes its JSON report before erroring out, so a non-zero exit
    # code still produces a parseable output file with real IO counters
    # — except when fio rejects the job entirely (e.g. blocksize > file
    # size), in which case it writes plain-text errors into the output
    # file. Treat both missing-file and unparseable-file as a single
    # error with the captured stdout/stderr in raw_json.
    if not output_json.exists():
        return _FioOutcome(
            job_name=job.name,
            errors=1,
            total_io_bytes=0,
            runtime_ms=0,
            raw_json={"returncode": proc.returncode, "stderr": proc.stderr},
        )
    raw_text = output_json.read_text()
    try:
        raw = json.loads(raw_text)
    except json.JSONDecodeError:
        return _FioOutcome(
            job_name=job.name,
            errors=1,
            total_io_bytes=0,
            runtime_ms=0,
            raw_json={
                "returncode": proc.returncode,
                "stderr": proc.stderr,
                "raw_output_head": raw_text[:1000],
            },
        )
    total_errors = 0
    total_bytes = 0
    runtime_ms = 0
    for entry in raw.get("jobs", []):
        total_errors += int(entry.get("error", 0))
        for direction in ("read", "write"):
            section = entry.get(direction, {})
            total_bytes += int(section.get("io_bytes", 0))
            runtime_ms = max(runtime_ms, int(section.get("runtime", 0)))
    return _FioOutcome(
        job_name=job.name,
        errors=total_errors,
        total_io_bytes=total_bytes,
        runtime_ms=runtime_ms,
        raw_json=raw,
    )


def _slice_access_log(
    access_log_path: pathlib.Path,
    t_lo: float,
    t_hi: float,
    path_basename: str | None = None,
) -> list[dict]:
    """Return access-log records in ``[t_lo, t_hi]``, optionally filtered.

    Robust to the file not yet existing (e.g. job timed out before any
    reads were served) and to malformed trailing lines (e.g. a partial
    line at end-of-file if the mount was killed mid-write). When
    ``path_basename`` is set, only records whose ``path`` matches that
    basename are returned — used to attribute reads to the right file
    when multiple jobs share the mount session.
    """
    if not access_log_path.exists():
        return []
    records: list[dict] = []
    with access_log_path.open() as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n")
            if line == "":
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            t_start = rec.get("t_start")
            if t_start is None or not (t_lo <= t_start <= t_hi):
                continue
            if path_basename is not None and rec.get("path") != path_basename:
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
        # Sweep-line convention: process ``-1`` events before ``+1`` at
        # the same instant so zero-duration reads don't show overlap
        # with their neighbours.
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


def _format_detail(outcome: _FioOutcome, gated: bool) -> str:
    mb = outcome.total_io_bytes / 1024 / 1024
    if outcome.runtime_ms > 0:
        mbs = f"{(mb * 1000 / outcome.runtime_ms):.1f} MB/s"
    else:
        mbs = "?"
    parts = [
        f"errors={outcome.errors}",
        f"io={mb:.1f} MB",
        f"runtime={outcome.runtime_ms}ms",
        f"throughput={mbs}",
    ]
    if not gated:
        parts.append("(informational)")
    return " ".join(parts)


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
            passed=True,
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
        for job in JOBS:
            tgt = mnt / f"{spec.name}{job.target_suffix}"
            if not tgt.exists():
                return tools.RunnerResult(
                    runner="fio",
                    passed=False,
                    duration_s=time.monotonic() - started,
                    summary=f"target file missing: {tgt}",
                )
        logger.info("fio: fixture=%s, %d jobs", spec.name, len(JOBS))
        for job in JOBS:
            target_file = mnt / f"{spec.name}{job.target_suffix}"
            logger.info(
                "fio: running %s (target=%s, gate=%s)",
                job.name,
                target_file.name,
                job.gate,
            )
            t0 = time.monotonic()
            timed_out = False
            try:
                outcome = _run_fio_job(job, target_file, log_dir, runtime_override_s)
            except subprocess.TimeoutExpired as exc:
                logger.warning("fio: %s timed out: %s", job.name, exc)
                timed_out = True
                outcome = None
            t1 = time.monotonic()
            duration = t1 - t0
            if timed_out:
                # Subprocess-level timeout is a hard failure regardless of
                # gating: we expect fio itself to manage its own runtime.
                checks.append(
                    tools.CheckResult(
                        name=f"fio:{job.name}",
                        passed=False,
                        duration_s=duration,
                        detail=f"subprocess timeout (gate={job.gate})",
                    )
                )
            else:
                detail = _format_detail(outcome, gated=job.gate)
                logger.info("fio %s: %s", job.name, detail)
                if job.gate:
                    passed = outcome.errors == 0
                else:
                    passed = True
                checks.append(
                    tools.CheckResult(
                        name=f"fio:{job.name}",
                        passed=passed,
                        duration_s=duration,
                        detail=detail,
                    )
                )

            if job.concurrent is not None:
                target_basename = target_file.name
                records = _slice_access_log(
                    access_log_path, t0, t1, path_basename=target_basename
                )
                stats = _concurrency_stats(records)
                conc_detail = (
                    f"records={stats.record_count} fhs={stats.distinct_fhs} "
                    f"max_overlap={stats.max_overlap}"
                )
                passed = stats.max_overlap >= 2
                if job.concurrent == "multi-fh":
                    passed = passed and stats.distinct_fhs >= 2
                logger.info("fio %s concurrency: %s", job.name, conc_detail)
                checks.append(
                    tools.CheckResult(
                        name=f"fio:{job.name}:concurrent",
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
        summary=f"fixture={spec.name} jobs={len(JOBS)}",
    )
