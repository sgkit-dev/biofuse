"""BOLT-LMM operations.

BOLT-LMM takes the three plink-1 files explicitly. ``--lmmInfOnly`` runs
the cheaper infinitesimal-only mixed model. ``--LDscoresUseChip`` skips
the bundled LD-scores reference (avoids a download / file-not-found
under our test fixture).
"""

from .base import Operation

OPERATIONS: tuple[Operation, ...] = (
    Operation(
        "bolt_lmm_inf",
        "bolt",
        "boltlmm",
        "--lmmInfOnly",
        (
            "--bed", "${bed}",
            "--bim", "${bim}",
            "--fam", "${fam}",
            "--phenoFile", "${aux:pheno}",
            "--phenoCol", "QT1",
            "--lmmInfOnly",
            "--LDscoresUseChip",
            "--statsFile", "${out}.stats.gz",
        ),
        aux=("pheno",),
        expensive=True,
    ),
)
