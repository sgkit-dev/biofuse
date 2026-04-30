"""bed-reader op runner for the IO-pattern study.

Invoked by ``run.py`` for ``Operation`` entries with ``tool='bedreader'``.
Each ``--op`` value selects a distinct access pattern; the runner opens
the plink fileset via :class:`bed_reader.open_bed` and exercises that
pattern, then writes a tiny JSON receipt to ``<out>.json`` so the
harness has something to confirm a successful run.
"""

import argparse
import json
import logging
import pathlib
import random

import numpy as np
from bed_reader import open_bed

logger = logging.getLogger(__name__)


def _full_scan(bed: open_bed) -> dict:
    arr = bed.read(dtype="int8")
    return {
        "shape": list(arr.shape),
        "n_missing": int(np.sum(arr < 0)),
    }


def _slice_variants_1k(bed: open_bed) -> dict:
    n_var = bed.sid_count
    step = max(n_var // 1000, 1)
    idx = np.arange(0, n_var, step)[:1000]
    arr = bed.read(index=np.s_[:, idx], dtype="int8")
    return {
        "shape": list(arr.shape),
        "n_variants_requested": int(idx.size),
    }


def _slice_samples_10(bed: open_bed) -> dict:
    n_iid = bed.iid_count
    sample_idx = np.arange(min(10, n_iid))
    arr = bed.read(index=np.s_[sample_idx, :], dtype="int8")
    return {
        "shape": list(arr.shape),
    }


def _random_10(bed: open_bed) -> dict:
    rng = random.Random(7)
    n_iid = bed.iid_count
    n_var = bed.sid_count
    var_idx = sorted({rng.randrange(n_var) for _ in range(10)})
    sample_idx = sorted({rng.randrange(n_iid) for _ in range(10)})
    arr = bed.read(
        index=np.s_[np.array(sample_idx), np.array(var_idx)], dtype="int8"
    )
    return {
        "shape": list(arr.shape),
        "var_idx": var_idx,
        "sample_idx": sample_idx,
    }


OPS = {
    "full_scan": _full_scan,
    "slice_variants_1k": _slice_variants_1k,
    "slice_samples_10": _slice_samples_10,
    "random_10": _random_10,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bfile", required=True, help="plink fileset prefix")
    parser.add_argument("--op", required=True, choices=sorted(OPS))
    parser.add_argument("--out", required=True, help="output prefix")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    bed_path = pathlib.Path(f"{args.bfile}.bed")
    bed = open_bed(bed_path)
    try:
        result = OPS[args.op](bed)
    finally:
        # bed-reader's open_bed has no explicit close, but releasing the
        # reference frees the underlying handle.
        del bed

    receipt = {"op": args.op, "bfile": args.bfile, "result": result}
    pathlib.Path(f"{args.out}.json").write_text(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
