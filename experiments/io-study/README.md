# IO-pattern study: plink1.9 / plink2 over biofuse

A research artefact, not a piece of biofuse itself. Builds a moderately
sized plink fileset, runs a broad cross-section of plink1.9 / plink2
operations against a biofuse mount, and produces:

- `results/summary.csv` — per-op exit codes / wall times.
- `results/metrics.csv` — long-format access-pattern metrics
  (one row per `(op, file)`).
- `results/metrics_per_op.csv` — wide format, one row per op.
- `results/timelines/<op>.txt` — ASCII strip plots of `.bed` reads.
- [`report.md`](report.md) — written-up findings.

## Quickstart

From the repo root:

```bash
# Build the fixture (~5 min on a modern laptop).
uv run python experiments/io-study/build_fixture.py

# Run the default operation matrix (~3 min).
uv run python experiments/io-study/run.py

# Or include the slow ops (--pca, --r2, --make-king, --grm-bin):
uv run python experiments/io-study/run.py --include-expensive

# Produce the metrics tables and timelines.
uv run python experiments/io-study/analyze.py
```

A single op can be re-run with `--only`:

```bash
uv run python experiments/io-study/run.py --only p19_freq p2_freq
```

## What gets committed

The scripts (`build_fixture.py`, `operations.py`, `run.py`,
`analyze.py`), this README, the report, and the `results/` CSVs and
ASCII timelines.

The fixture data (`data/`), raw traces (`traces/`), per-op runtime
trees (`_runtime/`), and aux files (`_aux/`) are not committed; they
are reproducible from the scripts.

## Files

- `build_fixture.py` — msprime → bio2zarr.tskit → vcztools.plink, then
  rewrite `.bim` to assign 22 synthetic chromosomes by position band.
- `operations.py` — the declarative list of plink invocations.
- `run.py` — for each op, mounts the golden dir via biofuse, generates
  any aux files, runs plink, writes a JSONL access trace.
- `analyze.py` — parses traces, emits `results/metrics*.csv` and
  ASCII timelines.
- `report.md` — written-up findings.
