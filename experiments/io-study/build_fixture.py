"""Build the IO-study fixture.

Runs msprime → bio2zarr.tskit.convert → vcztools.plink.write_plink to produce
a ~250 MiB plink fileset under ``data/golden``, then post-processes the .bim
to assign 22 synthetic chromosomes by position band and to give every variant
a unique ID. Idempotent: rereads ``data/fixture.json`` and skips work if the
parameters match.
"""

import argparse
import json
import logging
import pathlib
import shutil
import time

import bio2zarr.tskit as bio2zarr_tskit
import msprime
from vcztools import cli as vcztools_cli
from vcztools import plink as vcztools_plink

logger = logging.getLogger(__name__)

HERE = pathlib.Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
FIXTURE_FILE = DATA_DIR / "fixture.json"
GOLDEN_DIR = DATA_DIR / "golden"
GOLDEN_PREFIX = GOLDEN_DIR / "golden"
VCZ_PATH = DATA_DIR / "input.vcz"

DEFAULTS = {
    "num_diploid_samples": 1000,
    "sequence_length": 31_000_000,
    "mutation_rate": 1e-7,
    "recombination_rate": 1e-8,
    "population_size": 10_000,
    "ancestry_seed": 11,
    "mutation_seed": 12,
    "variants_chunk_size": 10_000,
    "samples_chunk_size": 1000,
    "n_chromosomes": 22,
}


def simulate_tree_sequence(params: dict):
    logger.info(
        "simulating tree sequence: %d samples × %.1f Mb",
        params["num_diploid_samples"],
        params["sequence_length"] / 1e6,
    )
    t0 = time.monotonic()
    ts = msprime.sim_ancestry(
        samples=params["num_diploid_samples"],
        sequence_length=params["sequence_length"],
        recombination_rate=params["recombination_rate"],
        population_size=params["population_size"],
        random_seed=params["ancestry_seed"],
        ploidy=2,
    )
    ts = msprime.sim_mutations(
        ts,
        rate=params["mutation_rate"],
        random_seed=params["mutation_seed"],
        model="binary",
    )
    elapsed = time.monotonic() - t0
    logger.info("simulated %d sites in %.1fs", ts.num_sites, elapsed)
    return ts


def write_vcz(ts, params: dict) -> None:
    logger.info("converting tree sequence to VCZ at %s", VCZ_PATH)
    t0 = time.monotonic()
    bio2zarr_tskit.convert(
        ts,
        VCZ_PATH,
        variants_chunk_size=params["variants_chunk_size"],
        samples_chunk_size=params["samples_chunk_size"],
    )
    logger.info("VCZ written in %.1fs", time.monotonic() - t0)


def write_plink_files() -> None:
    logger.info("materialising plink fileset at %s", GOLDEN_PREFIX)
    t0 = time.monotonic()
    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    reader = vcztools_cli.make_reader(str(VCZ_PATH))
    vcztools_plink.write_plink(reader, GOLDEN_PREFIX)
    logger.info("plink fileset written in %.1fs", time.monotonic() - t0)


def rewrite_bim_with_chromosomes(n_chromosomes: int) -> dict:
    """Assign synthetic chromosome IDs and unique variant IDs to the .bim file.

    Splits variants into n_chromosomes equal-width position bands, assigns
    CHROM = band_index + 1, and labels each variant ``rs<n>``. Returns a
    summary dict (variants per chromosome, position bounds).
    """
    bim_path = GOLDEN_PREFIX.with_suffix(".bim")
    logger.info("rewriting %s with %d chromosomes", bim_path, n_chromosomes)

    with bim_path.open() as f:
        rows = [line.rstrip("\n").split("\t") for line in f if line.strip()]

    positions = [int(r[3]) for r in rows]
    pos_min = min(positions)
    pos_max = max(positions)
    band_width = (pos_max - pos_min) // n_chromosomes + 1

    per_chrom_count: dict[int, int] = {}
    for i, row in enumerate(rows, start=1):
        position = int(row[3])
        chrom = min(((position - pos_min) // band_width) + 1, n_chromosomes)
        row[0] = str(chrom)
        row[1] = f"rs{i}"
        per_chrom_count[chrom] = per_chrom_count.get(chrom, 0) + 1

    with bim_path.open("w") as f:
        for row in rows:
            f.write("\t".join(row) + "\n")

    return {
        "n_variants": len(rows),
        "pos_min": pos_min,
        "pos_max": pos_max,
        "band_width": band_width,
        "per_chrom_count": dict(sorted(per_chrom_count.items())),
    }


def existing_fixture_matches(params: dict) -> bool:
    if not FIXTURE_FILE.exists():
        return False
    try:
        existing = json.loads(FIXTURE_FILE.read_text())
    except json.JSONDecodeError:
        return False
    relevant_keys = set(DEFAULTS) - {"variants_chunk_size", "samples_chunk_size"}
    for key in relevant_keys:
        if existing.get("params", {}).get(key) != params[key]:
            return False
    return all(
        (GOLDEN_DIR / f"golden{ext}").exists() for ext in (".bed", ".bim", ".fam")
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force", action="store_true", help="rebuild even if fixture matches"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    params = dict(DEFAULTS)

    if not args.force and existing_fixture_matches(params):
        logger.info(
            "fixture already up-to-date at %s; pass --force to rebuild", DATA_DIR
        )
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if VCZ_PATH.exists():
        shutil.rmtree(VCZ_PATH)
    if GOLDEN_DIR.exists():
        shutil.rmtree(GOLDEN_DIR)

    t0 = time.monotonic()
    ts = simulate_tree_sequence(params)
    write_vcz(ts, params)
    write_plink_files()
    chrom_summary = rewrite_bim_with_chromosomes(params["n_chromosomes"])
    elapsed = time.monotonic() - t0

    bed_path = GOLDEN_PREFIX.with_suffix(".bed")
    fam_path = GOLDEN_PREFIX.with_suffix(".fam")
    bim_path = GOLDEN_PREFIX.with_suffix(".bim")
    metadata = {
        "params": params,
        "elapsed_seconds": round(elapsed, 1),
        "num_variants": ts.num_sites,
        "num_diploid_samples": params["num_diploid_samples"],
        "bed_bytes": bed_path.stat().st_size,
        "bim_bytes": bim_path.stat().st_size,
        "fam_bytes": fam_path.stat().st_size,
        "chromosomes": chrom_summary,
    }
    FIXTURE_FILE.write_text(json.dumps(metadata, indent=2))
    logger.info(
        "fixture ready: %.1f MiB BED, %d variants, %d chromosomes (build %.1fs)",
        metadata["bed_bytes"] / (1 << 20),
        metadata["num_variants"],
        params["n_chromosomes"],
        elapsed,
    )


if __name__ == "__main__":
    main()
