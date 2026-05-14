"""Test helpers shared across the biofuse test suite."""

import pathlib
import shutil
from dataclasses import dataclass

import bio2zarr.tskit as bio2zarr_tskit
import msprime
import numpy as np
import zarr


@dataclass(frozen=True)
class VczFixture:
    """A built VCZ on disk plus the metadata used to build it."""

    path: pathlib.Path
    num_samples: int
    num_variants: int
    num_biallelic_sites: int
    variants_chunk_size: int
    samples_chunk_size: int


def _count_biallelic_sites(vcz_path: pathlib.Path) -> int:
    """Count sites a ``--max-alleles 2`` filter would keep.

    Mirrors :func:`vcztools.plink._check_biallelic`: a site is
    biallelic-acceptable iff every ``variant_allele`` column past index
    1 is empty (so ``alleles.shape[1] < 3`` short-circuits to "all
    biallelic").
    """
    store = zarr.open(vcz_path, mode="r")
    alleles = store["variant_allele"][:]
    if alleles.shape[1] < 3:
        return int(alleles.shape[0])
    extras = alleles[:, 2:]
    biallelic_mask = np.all(extras == "", axis=1)
    return int(np.sum(biallelic_mask))


def _keep_first_mutation_per_site(ts):
    """Drop second-and-later mutations at every site so the result is biallelic.

    msprime can place multiple mutations at the same site (recurrent
    mutation), which yields multi-allelic VCZ records that plink cannot
    represent. Keep the first mutation per site (table rows are already
    grouped by site) and rebuild the tree sequence.
    """
    tables = ts.dump_tables()
    sites = tables.mutations.site
    if len(sites) == 0:
        return ts
    keep = np.empty(len(sites), dtype=bool)
    keep[0] = True
    keep[1:] = sites[1:] != sites[:-1]
    tables.mutations.keep_rows(keep)
    tables.compute_mutation_parents()
    return tables.tree_sequence()


def simulate_vcz(
    out_dir: pathlib.Path,
    *,
    num_samples: int,
    sequence_length: float,
    mutation_rate: float,
    variants_chunk_size: int,
    samples_chunk_size: int,
    name: str = "sim",
    seed: int = 1,
    biallelic: bool = True,
    ploidy: int = 2,
) -> VczFixture:
    """Simulate a tree sequence and convert it to VCZ on disk.

    Returns a VczFixture pointing at a directory-format VCZ. The caller
    owns cleanup of out_dir. By default the fixture is biallelic: any
    site with multiple mutations keeps only its first mutation. Pass
    ``biallelic=False`` to keep recurrent mutations and exercise the
    multi-allelic rejection path. ``ploidy=1`` yields a VCZ with
    ``call_genotype`` of shape ``(V, num_samples, 1)``; the default
    ``ploidy=2`` yields ``(V, num_samples, 2)``.
    """
    ts = msprime.sim_ancestry(
        samples=num_samples,
        ploidy=ploidy,
        sequence_length=sequence_length,
        recombination_rate=1e-8,
        random_seed=seed,
    )
    ts = msprime.sim_mutations(
        ts,
        rate=mutation_rate,
        random_seed=seed + 1,
    )
    if biallelic:
        ts = _keep_first_mutation_per_site(ts)

    out_dir.mkdir(parents=True, exist_ok=True)
    vcz_path = out_dir / f"{name}.vcz"
    bio2zarr_tskit.convert(
        ts,
        vcz_path,
        variants_chunk_size=variants_chunk_size,
        samples_chunk_size=samples_chunk_size,
    )

    num_variants = ts.num_sites
    return VczFixture(
        path=vcz_path,
        num_samples=num_samples,
        num_variants=num_variants,
        num_biallelic_sites=_count_biallelic_sites(vcz_path),
        variants_chunk_size=variants_chunk_size,
        samples_chunk_size=samples_chunk_size,
    )


def mutate_to_mixed_ploidy(
    src_vcz: pathlib.Path,
    dst_dir: pathlib.Path,
    *,
    name: str = "mixed",
    haploid_sample_fraction: float = 0.5,
) -> VczFixture:
    """Copy a diploid VCZ and convert half its samples to effectively haploid.

    Writes the haploid sentinel ``-2`` into slot 1 of ``call_genotype`` for
    the first ``int(S * haploid_sample_fraction)`` samples across all
    variants. This is the canonical VCZ representation for samples with
    lower ploidy within a fixed-width genotype array — it is what an X /
    Y / MT chromosome looks like alongside diploid autosomes in the same
    store.
    """
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst_vcz = dst_dir / f"{name}.vcz"
    shutil.copytree(src_vcz, dst_vcz)

    store = zarr.open(dst_vcz, mode="r+")
    call_genotype = store["call_genotype"]
    num_variants, num_samples, _ = call_genotype.shape
    num_haploid = int(num_samples * haploid_sample_fraction)
    arr = call_genotype[:]
    arr[:, :num_haploid, 1] = -2
    call_genotype[:] = arr

    return VczFixture(
        path=dst_vcz,
        num_samples=num_samples,
        num_variants=num_variants,
        num_biallelic_sites=_count_biallelic_sites(dst_vcz),
        variants_chunk_size=call_genotype.chunks[0],
        samples_chunk_size=call_genotype.chunks[1],
    )
