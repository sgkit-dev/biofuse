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


@dataclasses.dataclass
class _FioOutcome:
    job_file: str
    errors: int
    total_io_bytes: int
    runtime_ms: int
    raw_json: dict


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
    checks: list[tools.CheckResult] = []
    with mount_mod.BiofuseMount(
        str(vcz_path), mountpoint, log_path=log_dir / "mount.log"
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
            try:
                outcome = _run_fio_job(
                    job_path, target_file, log_dir, runtime_override_s
                )
            except subprocess.TimeoutExpired as exc:
                logger.warning("fio: job %s timed out: %s", job_name, exc)
                checks.append(
                    tools.CheckResult(
                        name=f"fio:{job_name}",
                        passed=False,
                        duration_s=time.monotonic() - t0,
                        detail=f"timeout: {exc}",
                    )
                )
                continue
            duration = time.monotonic() - t0
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

    duration = time.monotonic() - started
    return tools.RunnerResult(
        runner="fio",
        passed=all(c.passed for c in checks),
        duration_s=duration,
        checks=checks,
        summary=f"fixture={spec.name} target=*.bed",
    )
