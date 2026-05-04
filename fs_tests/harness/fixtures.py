"""Cached VCZ fixtures for the harness.

Builds a small / medium / large VCZ via :func:`tests.helpers.simulate_vcz`
and caches them under ``fs_tests/.cache/`` keyed by parameters. Rebuilds
only when the cache directory is missing.
"""

import dataclasses
import logging
import pathlib
import shutil
import time

from tests import helpers

logger = logging.getLogger(__name__)

CACHE_DIR = pathlib.Path(__file__).resolve().parent.parent / ".cache"


@dataclasses.dataclass(frozen=True)
class FixtureSpec:
    name: str
    num_diploid_samples: int
    sequence_length: int
    mutation_rate: float
    variants_chunk_size: int
    samples_chunk_size: int
    seed: int


# Sizes are tuned so the resulting plink .bed file is large enough for fio's
# block-size sweep (64 KB blocks) without being so large that the simulation
# step dominates harness wall-clock time. The .bed file size is approximately
# num_variants * ceil(num_samples / 4); msprime variant counts scale with
# sequence_length * mutation_rate.

SMALL = FixtureSpec(
    name="small",
    num_diploid_samples=10,
    sequence_length=10_000,
    mutation_rate=1e-3,
    variants_chunk_size=7,
    samples_chunk_size=10,
    seed=11,
)

MEDIUM = FixtureSpec(
    name="medium",
    num_diploid_samples=200,
    sequence_length=10_000_000,
    mutation_rate=1e-3,
    variants_chunk_size=1000,
    samples_chunk_size=100,
    seed=23,
)

LARGE = FixtureSpec(
    name="large",
    num_diploid_samples=500,
    sequence_length=20_000_000,
    mutation_rate=2e-3,
    variants_chunk_size=2000,
    samples_chunk_size=250,
    seed=37,
)


def get_or_build(
    spec: FixtureSpec, cache_dir: pathlib.Path | None = None
) -> pathlib.Path:
    """Return path to the VCZ for ``spec``, building it if absent."""
    cache_dir = cache_dir or CACHE_DIR
    spec_dir = cache_dir / spec.name
    vcz_path = spec_dir / f"{spec.name}.vcz"
    marker = spec_dir / ".built"
    if marker.exists() and vcz_path.exists():
        logger.info("reusing cached fixture %s at %s", spec.name, vcz_path)
        return vcz_path
    logger.info(
        "building fixture %s under %s (samples=%d seq_len=%d mut_rate=%g)",
        spec.name,
        spec_dir,
        spec.num_diploid_samples,
        spec.sequence_length,
        spec.mutation_rate,
    )
    if spec_dir.exists():
        shutil.rmtree(spec_dir)
    build_started = time.monotonic()
    fixture = helpers.simulate_vcz(
        out_dir=spec_dir,
        num_diploid_samples=spec.num_diploid_samples,
        sequence_length=spec.sequence_length,
        mutation_rate=spec.mutation_rate,
        variants_chunk_size=spec.variants_chunk_size,
        samples_chunk_size=spec.samples_chunk_size,
        name=spec.name,
        seed=spec.seed,
    )
    marker.touch()
    bed_size_mb = fixture.num_variants * ((fixture.num_samples + 3) // 4) / 1024 / 1024
    logger.info(
        "fixture %s ready: variants=%d samples=%d bed_size~%.1f MB (%.1fs)",
        spec.name,
        fixture.num_variants,
        fixture.num_samples,
        bed_size_mb,
        time.monotonic() - build_started,
    )
    return vcz_path
