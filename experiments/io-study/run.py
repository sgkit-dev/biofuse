"""Run the IO-pattern operation matrix against a biofuse mount of the fixture.

For each operation:
1. Open a fresh AccessLogger writing JSONL to traces/<op_id>.jsonl.
2. Mount the golden plink directory through pyfuse3 via biofuse.
3. Generate any requested aux files (extract lists, phenotype) once.
4. Spawn the plink subprocess against the mount with a timeout.
5. Unmount, close logger, append a row to results/summary.csv.

Run with no arguments to skip ``expensive=True`` operations.
"""

import argparse
import csv
import json
import logging
import os
import pathlib
import random
import shutil
import string
import subprocess
import sys
import time

from biofuse import access_log, fuse_adapter, passthrough_view

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import operations as operations_module  # noqa: E402

DATA_DIR = HERE / "data"
GOLDEN_DIR = DATA_DIR / "golden"
GOLDEN_PREFIX_NAME = "golden"
TRACES_DIR = HERE / "traces"
RESULTS_DIR = HERE / "results"
RUN_DIR = HERE / "_runtime"
AUX_DIR = HERE / "_aux"

logger = logging.getLogger(__name__)


def _wait_for_mount(mnt: pathlib.Path, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.ismount(mnt):
            return
        time.sleep(0.05)
    raise RuntimeError(f"mountpoint {mnt} not live after {timeout}s")


# ---------------------------------------------------------------------------
# Aux file generators
# ---------------------------------------------------------------------------


def _read_bim_variant_ids() -> list[str]:
    """Read the rsN ids from the golden .bim. Cheap: ~25 MB file."""
    path = GOLDEN_DIR / f"{GOLDEN_PREFIX_NAME}.bim"
    ids = []
    with path.open() as f:
        for line in f:
            ids.append(line.split("\t", 2)[1])
    return ids


def _read_fam_sample_ids() -> list[tuple[str, str]]:
    path = GOLDEN_DIR / f"{GOLDEN_PREFIX_NAME}.fam"
    rows = []
    with path.open() as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            rows.append((parts[0], parts[1]))
    return rows


def _gen_extract_n(n: int):
    def _generator(out_path: pathlib.Path) -> None:
        ids = _read_bim_variant_ids()
        # Use evenly-spaced IDs to span the whole .bed (avoids
        # accidentally picking only the first few variants).
        if n >= len(ids):
            sample = ids
        else:
            step = len(ids) // n
            sample = [ids[i] for i in range(0, n * step, step)][:n]
        out_path.write_text("\n".join(sample) + "\n")

    return _generator


def _gen_keep_n(n: int):
    def _generator(out_path: pathlib.Path) -> None:
        rows = _read_fam_sample_ids()
        sample = rows[:n]
        with out_path.open("w") as f:
            for fid, iid in sample:
                f.write(f"{fid}\t{iid}\n")

    return _generator


def _gen_pheno(out_path: pathlib.Path) -> None:
    """Random quantitative phenotype, one per sample (FID IID PHENO)."""
    rng = random.Random(42)
    rows = _read_fam_sample_ids()
    with out_path.open("w") as f:
        f.write("FID\tIID\tQT1\n")
        for fid, iid in rows:
            f.write(f"{fid}\t{iid}\t{rng.gauss(0, 1):.6f}\n")


AUX_GENERATORS = {
    "extract_10": _gen_extract_n(10),
    "extract_1k": _gen_extract_n(1_000),
    "extract_100k": _gen_extract_n(100_000),
    "keep_10": _gen_keep_n(10),
    "pheno": _gen_pheno,
}


def _ensure_aux_files(names: tuple[str, ...]) -> dict[str, pathlib.Path]:
    AUX_DIR.mkdir(parents=True, exist_ok=True)
    paths: dict[str, pathlib.Path] = {}
    for name in names:
        if name not in AUX_GENERATORS:
            raise KeyError(f"unknown aux generator: {name}")
        path = AUX_DIR / name
        if not path.exists():
            logger.info("generating aux file %s", path)
            AUX_GENERATORS[name](path)
        paths[name] = path
    return paths


# ---------------------------------------------------------------------------
# Op execution
# ---------------------------------------------------------------------------


def _resolve_aux_placeholders(
    argv: tuple[str, ...], aux_paths: dict[str, pathlib.Path]
) -> list[str]:
    template = string.Template
    rendered = []
    for arg in argv:
        # Use simple manual substitution for ${aux:NAME}.
        if "${aux:" in arg:
            t = template(arg.replace("${aux:", "${aux_"))
            mapping = {f"aux_{k}": str(v) for k, v in aux_paths.items()}
            rendered.append(t.substitute(mapping))
        else:
            rendered.append(arg)
    return rendered


def _resolve_tool(name: str) -> str:
    p = shutil.which(name) or shutil.which(name.replace(".", ""))
    if p is None:
        raise RuntimeError(f"binary not found on PATH: {name}")
    return p


def run_operation(op: operations_module.Operation, *, timeout_s: int) -> dict:
    op_run = RUN_DIR / op.id
    if op_run.exists():
        shutil.rmtree(op_run)
    op_run.mkdir(parents=True)
    mnt = op_run / "mnt"
    mnt.mkdir()

    aux_paths = _ensure_aux_files(op.aux)
    tool_path = _resolve_tool(op.tool)
    out_prefix = op_run / "out"

    trace_path = TRACES_DIR / f"{op.id}.jsonl"
    if trace_path.exists():
        trace_path.unlink()

    rendered_argv = _resolve_aux_placeholders(op.argv, aux_paths)
    cmd = [
        tool_path,
        "--bfile",
        str(mnt / GOLDEN_PREFIX_NAME),
        "--out",
        str(out_prefix),
        *rendered_argv,
    ]

    logger.info("--- %s (%s, %s)", op.id, op.tool, op.label)
    logger.info("argv: %s", " ".join(cmd))

    log = access_log.AccessLogger(trace_path)
    view = passthrough_view.PassthroughDirectoryView(GOLDEN_DIR, access_logger=log)
    mount = fuse_adapter.Mount(view, str(mnt))
    mount.__enter__()
    elapsed = None
    exit_code = None
    timed_out = False
    stderr_tail = ""
    try:
        _wait_for_mount(mnt)
        t0 = time.monotonic()
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                check=False,
                timeout=timeout_s,
                cwd=op_run,
            )
            elapsed = time.monotonic() - t0
            exit_code = proc.returncode
            stderr_tail = (
                proc.stderr.decode("utf-8", errors="replace")[-512:]
                if proc.stderr
                else ""
            )
        except subprocess.TimeoutExpired as exc:
            elapsed = time.monotonic() - t0
            exit_code = -1
            timed_out = True
            stderr_tail = (exc.stderr or b"").decode("utf-8", errors="replace")[-512:]
    finally:
        mount.__exit__(None, None, None)
        view.close()
        log.close()

    return {
        "id": op.id,
        "tool": op.tool,
        "category": op.category,
        "label": op.label,
        "argv": " ".join(rendered_argv),
        "exit_code": exit_code,
        "timed_out": timed_out,
        "elapsed_s": round(elapsed or 0, 3),
        "trace": str(trace_path.relative_to(HERE)),
        "stderr_tail": stderr_tail.replace("\n", " ").replace("\t", " ")[:400],
    }


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--include-expensive", action="store_true")
    parser.add_argument(
        "--only", nargs="*", default=None, help="run only ops with these ids"
    )
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if not (DATA_DIR / "fixture.json").exists():
        sys.exit("fixture not built; run `uv run python build_fixture.py` first")

    TRACES_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    RUN_DIR.mkdir(parents=True, exist_ok=True)

    ops = list(operations_module.OPERATIONS)
    if not args.include_expensive:
        ops = [op for op in ops if not op.expensive]
    if args.only:
        wanted = set(args.only)
        ops = [op for op in ops if op.id in wanted]

    summary_path = RESULTS_DIR / "summary.csv"
    rows: list[dict] = []
    for op in ops:
        try:
            row = run_operation(op, timeout_s=args.timeout)
        except Exception as exc:
            logger.exception("operation %s raised", op.id)
            row = {
                "id": op.id,
                "tool": op.tool,
                "category": op.category,
                "label": op.label,
                "argv": "",
                "exit_code": "ERROR",
                "timed_out": False,
                "elapsed_s": 0,
                "trace": "",
                "stderr_tail": str(exc)[:400],
            }
        rows.append(row)

    fieldnames = [
        "id",
        "tool",
        "category",
        "label",
        "argv",
        "exit_code",
        "timed_out",
        "elapsed_s",
        "trace",
        "stderr_tail",
    ]
    with summary_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    failed = [r for r in rows if r["exit_code"] not in (0, "0")]
    logger.info("done: %d ops, %d failed", len(rows), len(failed))
    summary_meta = {
        "n_ops": len(rows),
        "n_failed": len(failed),
        "include_expensive": args.include_expensive,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    (RESULTS_DIR / "run_meta.json").write_text(json.dumps(summary_meta, indent=2))


if __name__ == "__main__":
    main()
