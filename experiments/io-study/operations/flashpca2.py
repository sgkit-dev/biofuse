"""flashpca2 operations.

flashpca takes ``--bfile prefix`` and writes PC scores / loadings to
the path given by ``--outpc``. We only test PCA; flashpca's other
modes share the same .bed access shape.
"""

from .base import Operation

OPERATIONS: tuple[Operation, ...] = (
    Operation(
        "flashpca_pca",
        "flashpca",
        "flashpca",
        "--ndim 10",
        (
            "--bfile", "${prefix}",
            "--ndim", "10",
            "--outpc", "${out}.pcs",
            "--outvec", "${out}.eigvecs",
            "--outval", "${out}.eigvals",
            "--outpve", "${out}.pve",
            "--outload", "${out}.loadings",
        ),
    ),
)
