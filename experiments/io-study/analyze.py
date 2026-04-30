"""Analyse JSONL access traces from the IO-pattern study.

Reads ``traces/<op_id>.jsonl`` files (produced by ``run.py``) and emits:
- ``results/metrics.csv`` — long format, one row per (op, file).
- ``results/metrics_per_op.csv`` — wide, one row per op (.bed columns
  prefixed bed_*, similarly bim_*, fam_*).
- ``results/timelines/<op_id>.txt`` — ASCII strip plot of read offset
  versus monotonic time, for the .bed file only. Intended for spot-check
  inclusion in the report.

Metrics per (op, file):
- bytes_read: ∑ read sizes
- n_reads
- min_size / median_size / p99_size / max_size
- unique_byte_coverage: ∑ size of merged-overlapping byte ranges
- coverage_pct: unique_byte_coverage / file_size
- redundancy: bytes_read / unique_byte_coverage  (1.0 = no re-reads)
- sequential_pct: % of consecutive read pairs where
  offset[i+1] == offset[i] + size[i]
- monotonic_pct: % where offset[i+1] >= offset[i]
- max_seek_back: largest backward jump (positive = backward) in bytes
- n_passes: # of indices where offset[i+1] < offset[i] (heuristic for
  "wrapped to start")
- elapsed_s: t_monotonic[-1] - t_monotonic[0]
"""

import argparse
import csv
import json
import logging
import pathlib
import statistics

HERE = pathlib.Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
TRACES_DIR = HERE / "traces"
RESULTS_DIR = HERE / "results"
TIMELINES_DIR = RESULTS_DIR / "timelines"
SUMMARY_PATH = RESULTS_DIR / "summary.csv"

logger = logging.getLogger(__name__)

FILES = ("golden.bed", "golden.bim", "golden.fam")


def _file_sizes() -> dict[str, int]:
    sizes = {}
    for name in FILES:
        sizes[name] = (DATA_DIR / "golden" / name).stat().st_size
    return sizes


def _merge_ranges(records: list[tuple[int, int]]) -> int:
    """Sum of sizes of merged-overlapping byte ranges."""
    if not records:
        return 0
    sorted_recs = sorted(records, key=lambda r: r[0])
    total = 0
    cur_start, cur_end = sorted_recs[0]
    for start, end in sorted_recs[1:]:
        if start <= cur_end:
            cur_end = max(cur_end, end)
        else:
            total += cur_end - cur_start
            cur_start, cur_end = start, end
    total += cur_end - cur_start
    return total


def _percentile(xs: list[int], q: float) -> float:
    if not xs:
        return 0
    s = sorted(xs)
    k = (len(s) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    if lo == hi:
        return float(s[lo])
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def analyse_trace(
    trace_path: pathlib.Path, file_sizes: dict[str, int]
) -> dict[str, dict]:
    """Return metrics keyed by file name."""
    by_file: dict[str, list[dict]] = {name: [] for name in FILES}
    with trace_path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec["path"] not in by_file:
                continue
            by_file[rec["path"]].append(rec)

    out = {}
    for name, recs in by_file.items():
        if not recs:
            out[name] = {"n_reads": 0}
            continue
        sizes = [r["size"] for r in recs]
        offsets = [r["offset"] for r in recs]
        ts = [r["t_monotonic"] for r in recs]

        ranges = [(o, o + s) for o, s in zip(offsets, sizes) if s > 0]
        unique_bytes = _merge_ranges(ranges)
        bytes_read = sum(sizes)

        seq_pairs = 0
        mono_pairs = 0
        max_back = 0
        n_passes = 0
        for i in range(len(recs) - 1):
            cur_end = offsets[i] + sizes[i]
            next_off = offsets[i + 1]
            if next_off == cur_end:
                seq_pairs += 1
            if next_off >= offsets[i]:
                mono_pairs += 1
            else:
                back = offsets[i] - next_off
                max_back = max(max_back, back)
                if next_off < offsets[i]:
                    # heuristic for wrap to a smaller offset
                    n_passes += 1
        n_pairs = max(len(recs) - 1, 1)

        file_size = file_sizes.get(name, 0)
        coverage_pct = 100.0 * unique_bytes / file_size if file_size else 0.0
        redundancy = bytes_read / unique_bytes if unique_bytes else 0.0
        out[name] = {
            "n_reads": len(recs),
            "bytes_read": bytes_read,
            "min_size": min(sizes),
            "median_size": int(statistics.median(sizes)),
            "p99_size": int(_percentile(sizes, 0.99)),
            "max_size": max(sizes),
            "unique_byte_coverage": unique_bytes,
            "coverage_pct": round(coverage_pct, 2),
            "redundancy": round(redundancy, 3),
            "sequential_pct": round(100.0 * seq_pairs / n_pairs, 1),
            "monotonic_pct": round(100.0 * mono_pairs / n_pairs, 1),
            "max_seek_back": max_back,
            "n_passes": n_passes,
            "elapsed_s": round(ts[-1] - ts[0], 3),
            "first_offset": offsets[0],
            "last_offset_end": offsets[-1] + sizes[-1],
        }
    return out


def emit_long_csv(rows: list[dict], path: pathlib.Path) -> None:
    fieldnames = [
        "id",
        "tool",
        "category",
        "label",
        "file",
        "n_reads",
        "bytes_read",
        "min_size",
        "median_size",
        "p99_size",
        "max_size",
        "unique_byte_coverage",
        "coverage_pct",
        "redundancy",
        "sequential_pct",
        "monotonic_pct",
        "max_seek_back",
        "n_passes",
        "elapsed_s",
        "first_offset",
        "last_offset_end",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def emit_per_op_csv(per_op: list[dict], path: pathlib.Path) -> None:
    """Wide format: one row per op with bed_*, bim_*, fam_* columns."""
    metric_cols = (
        "n_reads",
        "bytes_read",
        "median_size",
        "p99_size",
        "coverage_pct",
        "redundancy",
        "sequential_pct",
        "monotonic_pct",
        "max_seek_back",
        "n_passes",
        "elapsed_s",
    )
    fieldnames = ["id", "tool", "category", "label"]
    for prefix in ("bed", "bim", "fam"):
        for col in metric_cols:
            fieldnames.append(f"{prefix}_{col}")
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in per_op:
            writer.writerow(row)


def render_timeline(
    trace_path: pathlib.Path, file_size: int, width: int = 80, height: int = 12
) -> str:
    """ASCII strip plot of read offset (y) over read index (x)."""
    if file_size <= 0:
        return ""
    offsets = []
    with trace_path.open() as f:
        for line in f:
            rec = json.loads(line)
            if rec["path"] == "golden.bed":
                offsets.append(rec["offset"])
    if not offsets:
        return "(no .bed reads)\n"
    n = len(offsets)
    grid = [[" "] * width for _ in range(height)]
    for i, off in enumerate(offsets):
        x = min(int(i * (width - 1) / max(n - 1, 1)), width - 1)
        y = min(int(off * (height - 1) / max(file_size - 1, 1)), height - 1)
        grid[y][x] = "*"
    lines = ["".join(row) for row in grid]
    legend = (
        f"# .bed reads={n}  file_size={file_size:,}B  "
        f"(top=byte 0, bottom=EOF; left=first read, right=last)\n"
    )
    return legend + "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if not SUMMARY_PATH.exists():
        raise SystemExit(f"missing {SUMMARY_PATH}; run run.py first")

    file_sizes = _file_sizes()
    TIMELINES_DIR.mkdir(parents=True, exist_ok=True)

    long_rows: list[dict] = []
    per_op_rows: list[dict] = []

    with SUMMARY_PATH.open() as f:
        reader = csv.DictReader(f)
        for run_row in reader:
            op_id = run_row["id"]
            trace_rel = run_row.get("trace") or ""
            if not trace_rel:
                continue
            trace_path = HERE / trace_rel
            if not trace_path.exists():
                logger.warning("missing trace for %s: %s", op_id, trace_path)
                continue

            metrics = analyse_trace(trace_path, file_sizes)
            wide_row: dict = {
                "id": op_id,
                "tool": run_row["tool"],
                "category": run_row["category"],
                "label": run_row["label"],
            }
            for name, m in metrics.items():
                short = name.replace("golden.", "").replace(".", "")
                long_rows.append(
                    {
                        "id": op_id,
                        "tool": run_row["tool"],
                        "category": run_row["category"],
                        "label": run_row["label"],
                        "file": short,
                        **m,
                    }
                )
                for k, v in m.items():
                    if k in (
                        "n_reads",
                        "bytes_read",
                        "median_size",
                        "p99_size",
                        "coverage_pct",
                        "redundancy",
                        "sequential_pct",
                        "monotonic_pct",
                        "max_seek_back",
                        "n_passes",
                        "elapsed_s",
                    ):
                        wide_row[f"{short}_{k}"] = v
            per_op_rows.append(wide_row)

            timeline = render_timeline(trace_path, file_sizes["golden.bed"])
            (TIMELINES_DIR / f"{op_id}.txt").write_text(timeline)

    emit_long_csv(long_rows, RESULTS_DIR / "metrics.csv")
    emit_per_op_csv(per_op_rows, RESULTS_DIR / "metrics_per_op.csv")
    logger.info(
        "wrote %d ops × %d files to results/metrics*.csv",
        len(per_op_rows),
        len(FILES),
    )


if __name__ == "__main__":
    main()
