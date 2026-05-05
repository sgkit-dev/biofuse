"""fsx-style cross-validation runner against a live biofuse mount.

Apple-style fsx (and the LTP / xfstests variants) all assume a writable
filesystem: they construct their in-memory model by writing to the file
under test. Against a read-only FUSE mount such as biofuse, none of them
can run unmodified.

This runner reimplements the *core* of fsx for read-only mode in
Python: for N random operations, it picks pread or mmap-read with random
(offset, size) and compares the bytes returned by biofuse against an
oracle copy of the same file kept on the host filesystem.
"""

import logging
import mmap
import os
import pathlib
import random
import time

from . import fixtures, tools
from . import mount as mount_mod

logger = logging.getLogger(__name__)

SEEDS = [7, 23, 101]
DEFAULT_OPS_PER_SEED = 50_000
DEFAULT_MAX_OP_SIZE = 1 << 20  # 1 MiB


def _run_one_seed(
    *,
    oracle_bytes: bytes,
    target_fd: int,
    target_map: mmap.mmap | None,
    file_size: int,
    seed: int,
    n_ops: int,
    max_op_size: int,
    log_path: pathlib.Path,
) -> tools.CheckResult:
    """One fsx run: N random pread/mmap-read ops vs. oracle."""
    started = time.monotonic()
    rng = random.Random(seed)
    completed = 0
    mismatches = 0
    short_reads = 0
    mismatch_line: str | None = None

    logger.info("fsx: seed=%d ops=%d max_op_size=%d", seed, n_ops, max_op_size)
    for _ in range(n_ops):
        off = rng.randrange(file_size)
        max_size = min(max_op_size, file_size - off)
        size = rng.randrange(1, max_size + 1)
        do_mmap = target_map is not None and (rng.getrandbits(1) == 0)

        if do_mmap:
            data = bytes(target_map[off : off + size])
            expected = oracle_bytes[off : off + size]
            if data != expected:
                mismatches += 1
                mismatch_line = f"MAPREAD MISMATCH at offset=0x{off:x} size=0x{size:x}"
                break
        else:
            data = os.pread(target_fd, size, off)
            n = len(data)
            if n != size:
                short_reads += 1
            expected = oracle_bytes[off : off + n]
            if data != expected:
                mismatches += 1
                mismatch_line = f"READ MISMATCH at offset=0x{off:x} size=0x{size:x}"
                break
        completed += 1

    duration = time.monotonic() - started
    passed = mismatches == 0 and completed == n_ops

    log_lines = [
        f"seed={seed} ops={n_ops} max_op_size={max_op_size}",
        f"mmap_available={target_map is not None}",
    ]
    if mismatch_line is not None:
        log_lines.append(mismatch_line)
    log_lines.append(
        f"Completed {completed} of {n_ops} operations "
        f"(mismatches={mismatches} short_reads={short_reads})"
    )
    if passed:
        log_lines.append(f"All {n_ops} operations completed A-OK!")
    log_path.write_text("\n".join(log_lines) + "\n")

    detail = (
        f"completed={completed}/{n_ops} "
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

    spec = fixtures.MEDIUM
    vcz_path = fixtures.get_or_build(spec)

    scratch = log_dir / "oracle"
    oracle_bed = tools.materialise_plink_oracle(vcz_path, scratch, spec.name)
    oracle_bytes = oracle_bed.read_bytes()
    file_size = len(oracle_bytes)

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
        target_st = target_bed.stat()
        if target_st.st_size != file_size:
            return tools.RunnerResult(
                runner="fsx",
                passed=False,
                duration_s=time.monotonic() - started,
                summary=(
                    f"size mismatch: oracle={file_size} target={target_st.st_size}"
                ),
            )

        target_fd = os.open(str(target_bed), os.O_RDONLY | os.O_CLOEXEC)
        target_map: mmap.mmap | None = None
        try:
            try:
                target_map = mmap.mmap(target_fd, file_size, prot=mmap.PROT_READ)
            except (OSError, ValueError) as exc:
                logger.warning("mmap failed (%s); MAPREAD ops will be skipped", exc)
            for seed in seeds:
                checks.append(
                    _run_one_seed(
                        oracle_bytes=oracle_bytes,
                        target_fd=target_fd,
                        target_map=target_map,
                        file_size=file_size,
                        seed=seed,
                        n_ops=n_ops,
                        max_op_size=max_op_size,
                        log_path=log_dir / f"seed-{seed}.log",
                    )
                )
        finally:
            if target_map is not None:
                target_map.close()
            os.close(target_fd)

    duration = time.monotonic() - started
    return tools.RunnerResult(
        runner="fsx",
        passed=all(c.passed for c in checks),
        duration_s=duration,
        checks=checks,
        summary=f"fsx read-only cross-validation: {n_ops} ops × {len(seeds)} seeds",
    )
