# IO-pattern study: plink-1 binary consumers over biofuse

A research artefact, not a piece of biofuse itself. Builds a moderately
sized plink fileset, mounts it through biofuse with `direct_io=True`
(so any consumer's reads — including those routed through mmap — surface
to the access logger), and runs a representative cross-section of the
plink-1 binary ecosystem against the mount:

- **plink1.9** / **plink2** — the original two consumers (43 ops + 21
  threading variants).
- **ADMIXTURE**, **KING**, **GCTA**, **flashpca2**, **REGENIE**,
  **BOLT-LMM** — common downstream tools that read `.bed`/`.bim`/`.fam`
  via their own client code.
- **bed-reader** — Python library wrapper, exercised through four
  access shapes (`full_scan`, `slice_variants_1k`, `slice_samples_10`,
  `random_10`) via `scripts/bedreader_runner.py`.

Outputs:

- `results/summary.csv` — per-op exit codes / wall times / argv.
- `results/metrics.csv` — long-format access-pattern metrics
  (one row per `(op, file)`).
- `results/metrics_per_op.csv` — wide format, one row per op.
- `results/timelines/<op>.txt` — ASCII strip plots of `.bed` reads.
- [`report.md`](report.md) — written-up findings (covers the original
  plink1.9 / plink2 study; pending refresh with the wider zoo).

## Quickstart

From the repo root:

```bash
# 1. Install third-party tools into experiments/io-study/_tools/.
#    Idempotent; re-run with --force to refetch. Skips tools that fail
#    to download — the harness will mark their ops MISSING_TOOL rather
#    than aborting. Adds bed-reader through the `experiments` group.
uv sync --group experiments
uv run python experiments/io-study/install/install.py

# 2. Build the fixture (~5 min on a modern laptop).
uv run python experiments/io-study/build_fixture.py

# 3. Run the operation matrix (~3 min for fast ops; --include-expensive
#    runs the GRM / PCA / GWAS workloads too).
uv run python experiments/io-study/run.py
uv run python experiments/io-study/run.py --include-expensive

# 4. Produce metrics tables and timelines.
uv run python experiments/io-study/analyze.py
```

Selective re-runs:

```bash
# By op id:
uv run python experiments/io-study/run.py --only p19_freq bedreader_random_10

# By tool:
uv run python experiments/io-study/run.py --tool admixture king
```

## What gets committed

The scripts (`build_fixture.py`, `operations/`, `run.py`, `analyze.py`,
`install/`, `scripts/`), this README, the report, and the `results/`
CSVs and ASCII timelines.

The fixture data (`data/`), raw traces (`traces/`), per-op runtime
trees (`_runtime/`), aux files (`_aux/`), and downloaded tool binaries
(`_tools/`) are not committed; they are reproducible from the scripts.

## Adding a new tool

1. Add a `ToolSpec` to `install/registry.py` describing its
   distribution (URL, archive type, archive member glob, exe name).
2. Drop a `operations/<tool>.py` listing the ops that fingerprint the
   tool's access pattern. Use the substitution placeholders documented
   in `operations/__init__.py`.
3. Append `operations.<tool>.OPERATIONS` to the aggregation in
   `operations/__init__.py`.
4. (Optional, for libraries:) drop a `scripts/<tool>_runner.py` CLI
   wrapper and have the operation argv reference `${runner}`.

## Files

- `build_fixture.py` — msprime → bio2zarr.tskit → vcztools.plink, then
  rewrite `.bim` to assign 22 synthetic chromosomes by position band.
- `operations/` — per-tool declarative op lists; `operations/base.py`
  holds the shared `Operation` dataclass; `operations/__init__.py`
  documents the substitution grammar and aggregates `OPERATIONS`.
- `install/registry.py` + `install/install.py` — reusable installer.
- `scripts/bedreader_runner.py` — bed-reader op driver.
- `run.py` — for each op, mounts the golden dir via biofuse with
  `direct_io=True`, generates aux files, runs the consumer, writes a
  JSONL access trace.
- `analyze.py` — parses traces, emits `results/metrics*.csv` and
  ASCII timelines.
- `report.md` — written-up findings.
