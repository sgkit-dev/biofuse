"""Test helpers shared across the biofuse test suite."""

import io
import pathlib
from dataclasses import dataclass

import bio2zarr.vcf as bio2zarr_vcf
import msprime


@dataclass(frozen=True)
class VczFixture:
    """A built VCZ on disk plus the metadata used to build it."""

    path: pathlib.Path
    num_samples: int
    num_variants: int
    variants_chunk_size: int
    samples_chunk_size: int


def simulate_vcz(
    out_dir: pathlib.Path,
    *,
    num_diploid_samples: int,
    sequence_length: float,
    mutation_rate: float,
    variants_chunk_size: int,
    samples_chunk_size: int,
    name: str = "sim",
    seed: int = 1,
) -> VczFixture:
    """Simulate a tree sequence and convert it to VCZ on disk.

    Returns a VczFixture pointing at a directory-format VCZ. The caller owns
    cleanup of out_dir.
    """
    ts = msprime.sim_ancestry(
        samples=num_diploid_samples,
        sequence_length=sequence_length,
        recombination_rate=1e-8,
        random_seed=seed,
    )
    ts = msprime.sim_mutations(
        ts,
        rate=mutation_rate,
        random_seed=seed + 1,
        model="binary",
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    vcf_path = out_dir / f"{name}.vcf"
    with vcf_path.open("w") as f:
        ts.write_vcf(f)

    vcz_path = out_dir / f"{name}.vcz"
    bio2zarr_vcf.convert(
        [vcf_path],
        vcz_path,
        worker_processes=0,
        variants_chunk_size=variants_chunk_size,
        samples_chunk_size=samples_chunk_size,
    )

    num_variants = ts.num_sites
    num_samples = ts.num_samples // 2
    return VczFixture(
        path=vcz_path,
        num_samples=num_samples,
        num_variants=num_variants,
        variants_chunk_size=variants_chunk_size,
        samples_chunk_size=samples_chunk_size,
    )


def vcf_to_vcz(
    vcf_path: pathlib.Path,
    vcz_path: pathlib.Path,
    *,
    variants_chunk_size: int | None = None,
    samples_chunk_size: int | None = None,
) -> pathlib.Path:
    """Thin wrapper around bio2zarr.vcf.convert returning the output path."""
    bio2zarr_vcf.convert(
        [vcf_path],
        vcz_path,
        worker_processes=0,
        variants_chunk_size=variants_chunk_size,
        samples_chunk_size=samples_chunk_size,
    )
    return vcz_path


def write_vcf_for_tree_sequence(
    ts,
    path: pathlib.Path,
) -> pathlib.Path:
    """Write a tree sequence to a VCF file at path."""
    with path.open("w") as f:
        ts.write_vcf(f)
    return path


def read_bytes(path: pathlib.Path) -> bytes:
    """Convenience: read all bytes from a path."""
    return path.read_bytes()


def read_text(path: pathlib.Path) -> str:
    """Convenience: read all text from a path."""
    return path.read_text()


def vcf_to_string(ts) -> str:
    """Render a tree sequence to a VCF string (for ad-hoc inspection)."""
    buf = io.StringIO()
    ts.write_vcf(buf)
    return buf.getvalue()
