"""Command-line entrypoint for the fs_tests harness."""

import datetime
import logging
import pathlib
import subprocess
import sys

import click

from harness import (
    fio_runner,
    fsx_runner,
    lifecycle,
    pjdfstest,
    posix,
    report,
    stress_ng_runner,
    tools,
)

logger = logging.getLogger(__name__)

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
DEFAULT_RESULTS_ROOT = REPO_ROOT / "fs_tests" / "results"


def _git_head_commit() -> str:
    try:
        proc = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        if proc.returncode == 0:
            return proc.stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        pass
    return "<unknown>"


def _new_results_dir(root: pathlib.Path) -> pathlib.Path:
    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    out = root / stamp
    out.mkdir(parents=True, exist_ok=True)
    return out


@click.group(invoke_without_command=True)
@click.option("--quick", is_flag=True, help="Skip slow runners (pjdfstest, fio).")
@click.option("--large", is_flag=True, help="Use the 500MB fixture for fio.")
@click.option(
    "--results",
    type=click.Path(file_okay=False, path_type=pathlib.Path),
    default=None,
    help="Results root directory.",
)
@click.option(
    "-v",
    "--verbose",
    count=True,
    help="Increase log verbosity (repeat for debug).",
)
@click.pass_context
def main(
    ctx: click.Context,
    quick: bool,
    large: bool,
    results: pathlib.Path | None,
    verbose: int,
) -> None:
    """biofuse filesystem test harness."""
    level = max(logging.INFO - 10 * verbose, logging.DEBUG)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    results_root = results or DEFAULT_RESULTS_ROOT
    results_root.mkdir(parents=True, exist_ok=True)
    results_dir = _new_results_dir(results_root)
    ctx.obj = {
        "quick": quick,
        "large": large,
        "results_dir": results_dir,
    }
    logger.info("fs_tests harness starting; results dir: %s", results_dir)
    if ctx.invoked_subcommand is None:
        ctx.invoke(all_cmd)


def _emit(results_dir: pathlib.Path, results: list[tools.RunnerResult]) -> int:
    metadata = report.make_metadata(
        started_at=datetime.datetime.now(datetime.UTC),
        biofuse_commit=_git_head_commit(),
        tool_versions={
            "fio": tools.tool_version("fio"),
            "stress-ng": tools.tool_version("stress-ng"),
            "fusermount3": tools.tool_version("fusermount3", ["--version"]),
            "git": tools.tool_version("git"),
        },
    )
    report.write_reports(results_dir, results, metadata)
    md = (results_dir / "report.md").read_text()
    sys.stdout.write(md)
    if all(r.passed for r in results):
        return 0
    return 1


def _runner_log_dir(results_dir: pathlib.Path, name: str) -> pathlib.Path:
    log_dir = results_dir / name
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def _run_with_banner(
    name: str,
    fn,
) -> tools.RunnerResult:
    """Wrap a runner.run() call with INFO banners and a post-run summary."""
    logger.info("========== runner: %s ==========", name)
    result = fn()
    if result.skipped:
        logger.info("runner %s: SKIPPED — %s", name, result.skip_reason)
    else:
        verdict = "PASS" if result.passed else "FAIL"
        logger.info(
            "runner %s: %s (%d/%d passed in %.1fs)",
            name,
            verdict,
            result.num_passed,
            len(result.checks),
            result.duration_s,
        )
    return result


@main.command("posix")
@click.pass_context
def posix_cmd(ctx: click.Context) -> None:
    """Run only the native POSIX checks."""
    results_dir = ctx.obj["results_dir"]
    log_dir = _runner_log_dir(results_dir, "posix")
    result = _run_with_banner(
        "posix", lambda: posix.run(log_path=log_dir / "mount.log")
    )
    sys.exit(_emit(results_dir, [result]))


@main.command("pjdfstest")
@click.pass_context
def pjdfstest_cmd(ctx: click.Context) -> None:
    """Run only the pjdfstest curated subset."""
    results_dir = ctx.obj["results_dir"]
    log_dir = _runner_log_dir(results_dir, "pjdfstest")
    result = _run_with_banner("pjdfstest", lambda: pjdfstest.run(log_dir=log_dir))
    sys.exit(_emit(results_dir, [result]))


@main.command("fio")
@click.option(
    "--runtime",
    type=int,
    default=None,
    help="Override per-job runtime in seconds (default uses .fio file value).",
)
@click.pass_context
def fio_cmd(ctx: click.Context, runtime: int | None) -> None:
    """Run only the fio read-pattern stress."""
    results_dir = ctx.obj["results_dir"]
    log_dir = _runner_log_dir(results_dir, "fio")
    result = _run_with_banner(
        "fio",
        lambda: fio_runner.run(
            use_large_fixture=ctx.obj["large"],
            log_dir=log_dir,
            runtime_override_s=runtime,
        ),
    )
    sys.exit(_emit(results_dir, [result]))


@main.command("stress-ng")
@click.option(
    "--duration",
    type=float,
    default=30.0,
    help="Per-workload duration in seconds (default 30).",
)
@click.pass_context
def stress_ng_cmd(ctx: click.Context, duration: float) -> None:
    """Run only the stress-ng / open-loop stressors."""
    results_dir = ctx.obj["results_dir"]
    log_dir = _runner_log_dir(results_dir, "stress-ng")
    result = _run_with_banner(
        "stress-ng",
        lambda: stress_ng_runner.run(log_dir=log_dir, duration_s=duration),
    )
    sys.exit(_emit(results_dir, [result]))


@main.command("fsx")
@click.option(
    "--ops",
    type=int,
    default=fsx_runner.DEFAULT_OPS_PER_SEED,
    help="Operations per seed (default 50000).",
)
@click.option(
    "--max-op-size",
    type=int,
    default=fsx_runner.DEFAULT_MAX_OP_SIZE,
    help="Max read/mapread size in bytes (default 1 MiB).",
)
@click.pass_context
def fsx_cmd(ctx: click.Context, ops: int, max_op_size: int) -> None:
    """Run only the fsx-style read cross-validation."""
    results_dir = ctx.obj["results_dir"]
    log_dir = _runner_log_dir(results_dir, "fsx")
    result = _run_with_banner(
        "fsx",
        lambda: fsx_runner.run(log_dir=log_dir, n_ops=ops, max_op_size=max_op_size),
    )
    sys.exit(_emit(results_dir, [result]))


@main.command("lifecycle")
@click.option("--iterations", type=int, default=50)
@click.pass_context
def lifecycle_cmd(ctx: click.Context, iterations: int) -> None:
    """Run only the mount/unmount cycling stress."""
    results_dir = ctx.obj["results_dir"]
    log_dir = _runner_log_dir(results_dir, "lifecycle")
    result = _run_with_banner(
        "lifecycle",
        lambda: lifecycle.run(log_dir=log_dir, iterations=iterations),
    )
    sys.exit(_emit(results_dir, [result]))


@main.command("all")
@click.pass_context
def all_cmd(ctx: click.Context) -> None:
    """Run every runner and write an aggregated report."""
    results_dir = ctx.obj["results_dir"]
    quick = ctx.obj["quick"]
    use_large = ctx.obj["large"]

    posix_log = _runner_log_dir(results_dir, "posix") / "mount.log"
    pjd_log_dir = _runner_log_dir(results_dir, "pjdfstest")
    fio_log_dir = _runner_log_dir(results_dir, "fio")
    fsx_log_dir = _runner_log_dir(results_dir, "fsx")
    stress_log_dir = _runner_log_dir(results_dir, "stress-ng")
    lifecycle_log_dir = _runner_log_dir(results_dir, "lifecycle")

    results: list[tools.RunnerResult] = [
        _run_with_banner("posix", lambda: posix.run(log_path=posix_log))
    ]
    if not quick:
        results.append(
            _run_with_banner("pjdfstest", lambda: pjdfstest.run(log_dir=pjd_log_dir))
        )
        results.append(
            _run_with_banner(
                "fio",
                lambda: fio_runner.run(
                    use_large_fixture=use_large, log_dir=fio_log_dir
                ),
            )
        )
        results.append(
            _run_with_banner("fsx", lambda: fsx_runner.run(log_dir=fsx_log_dir))
        )
    results.append(
        _run_with_banner(
            "stress-ng", lambda: stress_ng_runner.run(log_dir=stress_log_dir)
        )
    )
    results.append(
        _run_with_banner("lifecycle", lambda: lifecycle.run(log_dir=lifecycle_log_dir))
    )

    sys.exit(_emit(results_dir, results))
