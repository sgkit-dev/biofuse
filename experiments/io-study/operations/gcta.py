"""GCTA operations.

GCTA shares plink's ``--bfile`` / ``--out`` convention and adds its own
analyses. Two ops fingerprint the BED access shape: ``--make-grm-bin``
(quadratic compute over genotypes) and ``--pca`` (uses the GRM).
"""

from .base import Operation

_BFILE = ("--bfile", "${prefix}", "--out", "${out}")


OPERATIONS: tuple[Operation, ...] = (
    Operation(
        "gcta_make_grm_bin",
        "gcta",
        "gcta",
        "--make-grm-bin",
        (*_BFILE, "--make-grm-bin"),
        expensive=True,
    ),
    Operation(
        "gcta_pca",
        "gcta",
        "gcta",
        "--pca 10 (via GRM)",
        (*_BFILE, "--make-grm-bin", "--pca", "10"),
        expensive=True,
    ),
)
