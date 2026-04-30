"""ADMIXTURE operations.

ADMIXTURE expects a .bed file as the first positional argument and a K
value (number of ancestral populations). It reads .bed/.bim/.fam from
the same directory as the .bed and writes outputs to ``cwd``. The
harness sets ``cwd`` to the per-op runtime dir so outputs land there.
"""

from .base import Operation

OPERATIONS: tuple[Operation, ...] = (
    Operation(
        "admixture_k3",
        "admixture",
        "admixture",
        "K=3 unsupervised",
        ("${bed}", "3"),
        expensive=True,
        timeout_s=1800,
    ),
)
