"""fsx-style cross-validation runner against a live biofuse mount.

Apple-style fsx (and the LTP / xfstests variants) all assume a writable
filesystem: they construct their in-memory model by writing to the file
under test. Against a read-only FUSE mount such as biofuse, none of them
can run unmodified.

This runner uses a small standalone C program (``tools/fsx_readonly.c``)
that implements the *core* of fsx for read-only mode: for N random
operations, it picks pread or mmap-read with random (offset, size) and
compares the bytes returned by biofuse against an oracle copy of the
same file kept on the host filesystem.

Build is one ``cc`` invocation on first run; binary is cached under
``fs_tests/.cache/fsx_readonly``.
"""

import logging
import pathlib
import re
import subprocess
import time

from harness import fixtures, tools
from harness import mount as mount_mod

logger = logging.getLogger(__name__)

SRC = pathlib.Path(__file__).resolve().parent.parent / "tools" / "fsx_readonly.c"
CACHE = pathlib.Path(__file__).resolve().parent.parent / ".cache"
BINARY = CACHE / "fsx_readonly"

# Pinned seeds for reproducibility. Three independent runs guard against
# a happy accident where one seed never visits the bug-trigger region.
SEEDS = [7, 23, 101]
DEFAULT_OPS_PER_SEED = 50_000
DEFAULT_MAX_OP_SIZE = 1 << 20  # 1 MiB

_ALL_OK_RE = re.compile(r"All (\d+) operations completed A-OK!")
_COMPLETED_RE = re.compile(
    r"Completed (\d+) of (\d+) operations \(mismatches=(\d+) short_reads=(\d+)\)"
)


def _ensure_built() -> str | None:
    """Compile fsx_readonly if needed. Returns None on success, reason on skip."""
    CACHE.mkdir(parents=True, exist_ok=True)
    if BINARY.exists() and BINARY.stat().st_mtime >= SRC.stat().st_mtime:
        return None
    if not tools.have_tool("cc"):
        return "cc not installed; cannot build fsx_readonly"
    cmd = ["cc", "-Wall", "-Wextra", "-O2", "-o", str(BINARY), str(SRC)]
    logger.info("building fsx_readonly: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, timeout=60, capture_output=True)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or b"").decode("utf-8", errors="replace")
        return f"build failed: {stderr[-500:]}"
    except subprocess.TimeoutExpired:
        return "build timed out"
    return None


def _run_one_seed(
    oracle_path: pathlib.Path,
    target_path: pathlib.Path,
    seed: int,
    n_ops: int,
    max_op_size: int,
    log_dir: pathlib.Path,
) -> tools.CheckResult:
    started = time.monotonic()
    cmd = [
        str(BINARY),
        str(oracle_path),
        str(target_path),
        str(n_ops),
        str(seed),
        str(max_op_size),
    ]
    logger.info("fsx: seed=%d ops=%d max_op_size=%d", seed, n_ops, max_op_size)
    logger.debug("fsx cmd: %s", " ".join(cmd))
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=600, check=False
        )
    except subprocess.TimeoutExpired as exc:
        logger.warning("fsx seed=%d timed out: %s", seed, exc)
        return tools.CheckResult(
            name=f"fsx:seed-{seed}",
            passed=False,
            duration_s=time.monotonic() - started,
            detail=f"timeout: {exc}",
        )

    log_path = log_dir / f"seed-{seed}.log"
    log_path.write_text(
        f"$ {' '.join(cmd)}\n"
        f"--- stdout ---\n{proc.stdout}\n"
        f"--- stderr ---\n{proc.stderr}\n"
        f"--- returncode: {proc.returncode}\n"
    )

    duration = time.monotonic() - started
    completed = mismatches = short_reads = 0
    m = _COMPLETED_RE.search(proc.stdout)
    if m:
        completed = int(m.group(1))
        mismatches = int(m.group(3))
        short_reads = int(m.group(4))
    all_ok = bool(_ALL_OK_RE.search(proc.stdout))
    passed = proc.returncode == 0 and all_ok and mismatches == 0
    detail = (
        f"rc={proc.returncode} completed={completed}/{n_ops} "
        f"mismatches={mismatches} short_reads={short_reads}"
    )
    logger.info("fsx seed=%d: %s", seed, detail)
    return tools.CheckResult(
        name=f"fsx:seed-{seed}",
        passed=passed,
        duration_s=duration,
        detail=detail,
    )


def run(
    *,
    log_dir: pathlib.Path,
    n_ops: int = DEFAULT_OPS_PER_SEED,
    max_op_size: int = DEFAULT_MAX_OP_SIZE,
    seeds: list[int] | None = None,
) -> tools.RunnerResult:
    started = time.monotonic()
    log_dir.mkdir(parents=True, exist_ok=True)
    seeds = seeds or list(SEEDS)

    skip_reason = _ensure_built()
    if skip_reason is not None:
        logger.info("fsx: SKIP (%s)", skip_reason)
        return tools.RunnerResult(
            runner="fsx",
            passed=True,
            duration_s=time.monotonic() - started,
            skipped=True,
            skip_reason=skip_reason,
        )

    spec = fixtures.MEDIUM
    vcz_path = fixtures.get_or_build(spec)

    # Materialise the plink fileset to a host-fs scratch dir as the
    # oracle; mount biofuse separately and point fsx at the mounted .bed.
    scratch = log_dir / "oracle"
    oracle_bed = tools.materialise_plink_oracle(vcz_path, scratch, spec.name)

    mountpoint = log_dir / "mnt"
    checks: list[tools.CheckResult] = []
    with mount_mod.BiofuseMount(
        str(vcz_path), mountpoint, log_path=log_dir / "mount.log"
    ) as mnt:
        target_bed = mnt / f"{spec.name}.bed"
        if not target_bed.exists():
            return tools.RunnerResult(
                runner="fsx",
                passed=False,
                duration_s=time.monotonic() - started,
                summary=f"target file missing: {target_bed}",
            )
        for seed in seeds:
            checks.append(
                _run_one_seed(oracle_bed, target_bed, seed, n_ops, max_op_size, log_dir)
            )
    duration = time.monotonic() - started
    return tools.RunnerResult(
        runner="fsx",
        passed=all(c.passed for c in checks),
        duration_s=duration,
        checks=checks,
        summary=f"fsx_readonly cross-validation: {n_ops} ops × {len(seeds)} seeds",
    )
