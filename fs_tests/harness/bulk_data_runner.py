"""Bulk-data cross-validation: mount bytes vs. encoder-direct bytes.

For both formats (plink, bgen), this runner:

1. Builds the streaming encoder (``BedEncoder`` / ``BgenEncoder``)
   directly against the fixture VCZ, and reads the first 100 MB as the
   oracle bytes.
2. Mounts the same VCZ via ``biofuse mount-<format>`` and reads the
   first 100 MB of the streaming file from the mountpoint.
3. Asserts the byte streams are identical.

If the encoder's ``total_size`` is below 100 MB, the read length is
clipped to ``total_size`` for both sides — only the leading
``min(total_size, 100 MB)`` of the oracle is materialised, keeping
memory bounded.
"""

import logging
import pathlib
import time

import vcztools

from . import fixtures, tools
from . import mount as mount_mod

logger = logging.getLogger(__name__)

CAP_BYTES = 100 * 1024 * 1024


def _build_encoder(format_name: str, vcz_path: pathlib.Path):
    if format_name == "plink":
        opts = vcztools.ViewPlinkOptions()
        reader = opts.make_reader(str(vcz_path))
        return vcztools.BedEncoder(reader)
    if format_name == "bgen":
        opts = vcztools.ViewBgenOptions()
        reader = opts.make_reader(str(vcz_path))
        return vcztools.BgenEncoder(reader)
    raise ValueError(f"unknown format {format_name!r}")


def _read_mount_prefix(path: pathlib.Path, size: int) -> bytes:
    """Read up to ``size`` bytes from ``path``, looping until EOF or
    the cap is reached."""
    chunks: list[bytes] = []
    remaining = size
    with open(path, "rb") as fh:
        while remaining > 0:
            chunk = fh.read(remaining)
            if len(chunk) == 0:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
    return b"".join(chunks)


def _check_one(
    *,
    format_name: str,
    streaming_suffix: str,
    vcz_path: pathlib.Path,
    basename: str,
    log_dir: pathlib.Path,
) -> tools.CheckResult:
    started = time.monotonic()
    log_lines: list[str] = [f"format={format_name} basename={basename}"]

    with _build_encoder(format_name, vcz_path) as encoder:
        total_size = encoder.total_size
        compare_size = min(total_size, CAP_BYTES)
        log_lines.append(
            f"encoder total_size={total_size} compare_size={compare_size} "
            f"(cap={CAP_BYTES})"
        )
        expected = encoder.read(0, compare_size)

    if len(expected) != compare_size:
        log_lines.append(
            f"encoder short read: got {len(expected)} expected {compare_size}"
        )
        (log_dir / f"{format_name}.log").write_text("\n".join(log_lines) + "\n")
        return tools.CheckResult(
            name=f"bulk-data:{format_name}",
            passed=False,
            duration_s=time.monotonic() - started,
            detail=f"encoder short read: {len(expected)}/{compare_size}",
        )

    mountpoint = log_dir / f"mnt-{format_name}"
    with mount_mod.BiofuseMount(
        str(vcz_path),
        mountpoint,
        format_name=format_name,
        basename=basename,
        log_path=log_dir / f"{format_name}-mount.log",
    ) as mnt:
        target = mnt / f"{basename}{streaming_suffix}"
        if not target.exists():
            log_lines.append(f"target missing: {target}")
            (log_dir / f"{format_name}.log").write_text("\n".join(log_lines) + "\n")
            return tools.CheckResult(
                name=f"bulk-data:{format_name}",
                passed=False,
                duration_s=time.monotonic() - started,
                detail=f"target file missing: {target.name}",
            )
        mount_size = target.stat().st_size
        log_lines.append(f"mount file size={mount_size}")
        if mount_size != total_size:
            (log_dir / f"{format_name}.log").write_text("\n".join(log_lines) + "\n")
            return tools.CheckResult(
                name=f"bulk-data:{format_name}",
                passed=False,
                duration_s=time.monotonic() - started,
                detail=f"size mismatch: encoder={total_size} mount={mount_size}",
            )
        actual = _read_mount_prefix(target, compare_size)

    duration = time.monotonic() - started

    if len(actual) != compare_size:
        log_lines.append(f"mount short read: {len(actual)}/{compare_size}")
        (log_dir / f"{format_name}.log").write_text("\n".join(log_lines) + "\n")
        return tools.CheckResult(
            name=f"bulk-data:{format_name}",
            passed=False,
            duration_s=duration,
            detail=f"mount short read: {len(actual)}/{compare_size}",
        )

    if actual != expected:
        first_diff = next(
            (i for i, (a, b) in enumerate(zip(actual, expected)) if a != b),
            min(len(actual), len(expected)),
        )
        log_lines.append(f"BYTE MISMATCH at offset 0x{first_diff:x}")
        (log_dir / f"{format_name}.log").write_text("\n".join(log_lines) + "\n")
        return tools.CheckResult(
            name=f"bulk-data:{format_name}",
            passed=False,
            duration_s=duration,
            detail=f"byte mismatch at offset 0x{first_diff:x}",
        )

    log_lines.append(f"OK: {compare_size} bytes match ({duration:.1f}s)")
    (log_dir / f"{format_name}.log").write_text("\n".join(log_lines) + "\n")
    logger.info(
        "bulk-data %s: %d bytes match (%.1fs)", format_name, compare_size, duration
    )
    return tools.CheckResult(
        name=f"bulk-data:{format_name}",
        passed=True,
        duration_s=duration,
        detail=f"compared {compare_size} bytes (encoder total_size={total_size})",
    )


def run(*, log_dir: pathlib.Path) -> tools.RunnerResult:
    started = time.monotonic()
    log_dir.mkdir(parents=True, exist_ok=True)

    spec = fixtures.MEDIUM
    vcz_path = fixtures.get_or_build(spec)

    checks: list[tools.CheckResult] = []
    for format_name, streaming_suffix in (("plink", ".bed"), ("bgen", ".bgen")):
        checks.append(
            _check_one(
                format_name=format_name,
                streaming_suffix=streaming_suffix,
                vcz_path=vcz_path,
                basename=spec.name,
                log_dir=log_dir,
            )
        )

    duration = time.monotonic() - started
    return tools.RunnerResult(
        runner="bulk-data",
        passed=all(c.passed for c in checks),
        duration_s=duration,
        checks=checks,
        summary=(
            f"bulk-data cross-validation: encoder vs mount, "
            f"cap={CAP_BYTES // (1024 * 1024)} MB"
        ),
    )
