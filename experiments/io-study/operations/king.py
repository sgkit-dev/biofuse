"""KING operations.

KING reads a plink-1 fileset via ``-b prefix.bed`` (it derives the .bim
and .fam paths from the .bed location) and writes ``<prefix>.kin0`` /
``<prefix>.kin``. ``--prefix`` controls the output prefix.
"""

from .base import Operation

OPERATIONS: tuple[Operation, ...] = (
    Operation(
        "king_kinship",
        "king",
        "king",
        "--kinship",
        ("-b", "${bed}", "--kinship", "--prefix", "${out}"),
    ),
)
