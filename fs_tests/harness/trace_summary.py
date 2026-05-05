"""Tiny trace summariser for an access.jsonl produced under -vv.

Reads a JSONL file produced by :class:`biofuse.access_log.AccessLogger`
(reads + lifecycle events with a ``kind`` field) and prints
per-event-kind duration distributions plus the top-N slowest events.

Run as::

    uv run python -m fs_tests.harness.trace_summary <path/to/access.jsonl>
"""

import argparse
import json
import pathlib
import sys
from collections import defaultdict


def _load(path: pathlib.Path) -> list[dict]:
    rows: list[dict] = []
    with path.open() as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line == "":
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def _percentiles(values: list[float], pcts: list[int]) -> list[float]:
    if len(values) == 0:
        return [0.0] * len(pcts)
    sorted_vals = sorted(values)
    out: list[float] = []
    for p in pcts:
        # Nearest-rank inclusive percentile.
        idx = max(0, min(len(sorted_vals) - 1, (p * len(sorted_vals) - 1) // 100))
        out.append(sorted_vals[idx])
    return out


def _print_kind_table(rows: list[dict]) -> None:
    by_kind: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        kind = r.get("kind", "read")
        try:
            duration = float(r["t_end"]) - float(r["t_start"])
        except (KeyError, TypeError, ValueError):
            continue
        by_kind[kind].append(duration)

    print(f"{'kind':<16} {'n':>6} {'p50':>8} {'p90':>8} {'p99':>8} {'max':>8}")
    print("-" * 60)
    for kind in sorted(by_kind):
        vs = by_kind[kind]
        p50, p90, p99 = _percentiles(vs, [50, 90, 99])
        print(
            f"{kind:<16} {len(vs):>6} "
            f"{p50:>8.3f} {p90:>8.3f} {p99:>8.3f} {max(vs):>8.3f}"
        )


def _print_topn_slowest(rows: list[dict], kind: str, n: int = 5) -> None:
    of_kind = [r for r in rows if r.get("kind", "read") == kind]
    if len(of_kind) == 0:
        return
    sized = []
    for r in of_kind:
        try:
            d = float(r["t_end"]) - float(r["t_start"])
        except (KeyError, TypeError, ValueError):
            continue
        sized.append((d, r))
    sized.sort(reverse=True)
    print(f"\nslowest {n} '{kind}' events:")
    for d, r in sized[:n]:
        print(
            f"  {d:>7.3f}s  fh={r.get('fh', '?'):<5} "
            f"path={r.get('path', '?')}  t_start={r.get('t_start', 0):.3f}"
        )


def _print_per_fh_summary(rows: list[dict]) -> None:
    by_fh: dict[int, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        try:
            fh = int(r.get("fh", -1))
            kind = r.get("kind", "read")
            d = float(r["t_end"]) - float(r["t_start"])
        except (KeyError, TypeError, ValueError):
            continue
        by_fh[fh][kind].append(d)

    if len(by_fh) == 0:
        return
    print("\nper-fh totals (s):")
    print(
        f"  {'fh':>4} {'open':>8} {'limiter':>8} "
        f"{'reads':>8} {'release':>8} {'aclose':>8}"
    )
    for fh in sorted(by_fh):
        kinds = by_fh[fh]
        opn = sum(kinds.get("open", []))
        lim = sum(kinds.get("limiter_wait", []))
        rds = sum(kinds.get("read", []))
        rel = sum(kinds.get("release", []))
        acl = sum(kinds.get("aclose", []))
        print(f"  {fh:>4} {opn:>8.3f} {lim:>8.3f} {rds:>8.3f} {rel:>8.3f} {acl:>8.3f}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=pathlib.Path, help="path to access.jsonl")
    parser.add_argument(
        "--top", type=int, default=5, help="how many slowest events to print"
    )
    args = parser.parse_args(argv)

    rows = _load(args.path)
    if len(rows) == 0:
        print(f"no records in {args.path}", file=sys.stderr)
        return 1

    print(f"loaded {len(rows)} records from {args.path}")
    print()
    _print_kind_table(rows)
    for kind in ("aclose", "release", "limiter_wait", "open", "read"):
        _print_topn_slowest(rows, kind, args.top)
    _print_per_fh_summary(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
