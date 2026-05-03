"""REGENIE operations.

Step 1 fits a whole-genome ridge model on a subset of variants; this is
the BED-reading step. ``--bsize`` is the variant block size; smaller
block sizes increase the per-block read overhead.
"""

from .base import Operation

OPERATIONS: tuple[Operation, ...] = (
    Operation(
        "regenie_step1_bt",
        "regenie",
        "regenie",
        "--step 1 --bt --lowmem",
        (
            "--step",
            "1",
            "--bed",
            "${prefix}",
            "--phenoFile",
            "${aux:pheno_binary}",
            "--bsize",
            "1000",
            "--bt",
            "--lowmem",
            "--lowmem-prefix",
            "${out}_lowmem",
            "--out",
            "${out}",
        ),
        aux=("pheno_binary",),
        expensive=True,
    ),
)
