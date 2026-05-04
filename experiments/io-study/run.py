"""Run the IO-pattern operation matrix against a biofuse mount of the fixture.

For each operation:
1. Open a fresh AccessLogger writing JSONL to traces/<op_id>.jsonl.
2. Mount the golden plink directory through pyfuse3 (kernel page cache
   on; ``direct_io`` was tried and rejected — see report.md).
3. Generate any requested aux files (extract lists, phenotype) once.
4. Spawn the consumer subprocess against the mount with a timeout.
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
TOOLS_DIR = HERE / "_tools"
TOOLS_MANIFEST = TOOLS_DIR / "manifest.json"
SCRIPTS_DIR = HERE / "scripts"

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


def _gen_pheno_binary(out_path: pathlib.Path) -> None:
    """Case/control phenotype for REGENIE/BOLT-LMM --bt; values in {0,1}."""
    rng = random.Random(43)
    rows = _read_fam_sample_ids()
    with out_path.open("w") as f:
        f.write("FID\tIID\tY1\n")
        for fid, iid in rows:
            f.write(f"{fid}\t{iid}\t{rng.randint(0, 1)}\n")


AUX_GENERATORS = {
    "extract_10": _gen_extract_n(10),
    "extract_1k": _gen_extract_n(1_000),
    "extract_100k": _gen_extract_n(100_000),
    "keep_10": _gen_keep_n(10),
    "pheno": _gen_pheno,
    "pheno_binary": _gen_pheno_binary,
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
# Tool resolution
# ---------------------------------------------------------------------------


def _load_tools_manifest() -> dict[str, str]:
    if not TOOLS_MANIFEST.exists():
        return {}
    return json.loads(TOOLS_MANIFEST.read_text())


# Tools whose binary names differ from the logical tool name in operations.
_PATH_FALLBACK_NAMES = {
    "plink1.9": ("plink1.9", "plink19", "plink"),
    "plink2": ("plink2",),
    "admixture": ("admixture",),
    "king": ("king",),
    "gcta": ("gcta64", "gcta"),
    "flashpca": ("flashpca",),
    "regenie": ("regenie",),
    "bolt": ("bolt",),
    # Library wrappers run as `uv run python <runner>`.
    "bedreader": ("bedreader",),
}


def _resolve_tool(name: str, manifest: dict[str, str]) -> str | None:
    """Return an executable path for ``name`` or None if unavailable.

    Precedence: ``_tools/manifest.json`` first, then ``$PATH`` over a small
    set of conventional binary names.
    """
    if name in manifest:
        return manifest[name]
    for candidate in _PATH_FALLBACK_NAMES.get(name, (name,)):
        path = shutil.which(candidate)
        if path is not None:
            return path
    return None


def _build_command(
    op: operations_module.Operation,
    *,
    tool_path: str,
    substitutions: dict[str, str],
) -> list[str]:
    """Render the op's argv template and prepend the tool/runner prefix.

    Library-style ops (``tool='bedreader'``) have ``${runner}`` in argv;
    we render it and run via ``uv run python <runner>`` so the script
    picks up the project's environment.
    """
    rendered = [_render(arg, substitutions) for arg in op.argv]
    if op.tool == "bedreader":
        return ["uv", "run", "python", *rendered]
    return [tool_path, *rendered]


def _render(arg: str, substitutions: dict[str, str]) -> str:
    """Substitute ``${name}`` and ``${aux:NAME}`` placeholders.

    Implemented via ``string.Template`` after rewriting the colon form
    so it can use the standard syntax.
    """
    if "$" not in arg:
        return arg
    template = string.Template(arg.replace("${aux:", "${aux_"))
    mapping = {}
    for k, v in substitutions.items():
        if k.startswith("aux:"):
            mapping[f"aux_{k[len('aux:') :]}"] = v
        else:
            mapping[k] = v
    return template.substitute(mapping)


# ---------------------------------------------------------------------------
# Op execution
# ---------------------------------------------------------------------------


def run_operation(
    op: operations_module.Operation,
    *,
    manifest: dict[str, str],
) -> dict:
    op_run = RUN_DIR / op.id
    if op_run.exists():
        shutil.rmtree(op_run)
    op_run.mkdir(parents=True)
    mnt = op_run / "mnt"
    mnt.mkdir()

    tool_path = _resolve_tool(op.tool, manifest)
    aux_paths = _ensure_aux_files(op.aux)
    out_prefix = op_run / "out"

    trace_path = TRACES_DIR / f"{op.id}.jsonl"
    if trace_path.exists():
        trace_path.unlink()

    substitutions = {
        "prefix": str(mnt / GOLDEN_PREFIX_NAME),
        "bed": str(mnt / f"{GOLDEN_PREFIX_NAME}.bed"),
        "bim": str(mnt / f"{GOLDEN_PREFIX_NAME}.bim"),
        "fam": str(mnt / f"{GOLDEN_PREFIX_NAME}.fam"),
        "mnt": str(mnt),
        "out": str(out_prefix),
        "runner": str(SCRIPTS_DIR / f"{op.tool}_runner.py"),
        **{f"aux:{k}": str(v) for k, v in aux_paths.items()},
    }

    base_row = {
        "id": op.id,
        "tool": op.tool,
        "category": op.category,
        "label": op.label,
    }

    if tool_path is None and op.tool != "bedreader":
        logger.warning("skipping %s: tool %r not found", op.id, op.tool)
        return {
            **base_row,
            "argv": "",
            "exit_code": "MISSING_TOOL",
            "timed_out": False,
            "elapsed_s": 0,
            "trace": "",
            "stderr_tail": f"tool {op.tool!r} not in manifest or $PATH",
        }

    cmd = _build_command(op, tool_path=tool_path or "", substitutions=substitutions)

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
                timeout=op.timeout_s,
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
        **base_row,
        "argv": " ".join(cmd),
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
    parser.add_argument(
        "--tool",
        nargs="*",
        default=None,
        help="run only ops whose tool name is in this list",
    )
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

    manifest = _load_tools_manifest()

    ops = list(operations_module.OPERATIONS)
    if not args.include_expensive:
        ops = [op for op in ops if not op.expensive]
    if args.only:
        wanted = set(args.only)
        ops = [op for op in ops if op.id in wanted]
    if args.tool:
        wanted_tools = set(args.tool)
        ops = [op for op in ops if op.tool in wanted_tools]

    summary_path = RESULTS_DIR / "summary.csv"
    rows: list[dict] = []
    for op in ops:
        try:
            row = run_operation(op, manifest=manifest)
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
