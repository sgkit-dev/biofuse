"""Operation matrix for the IO-pattern study.

Each ``Operation`` describes one external invocation against a biofuse
mount. The harness in ``run.py`` renders the ``argv`` template with the
following substitutions:

- ``${prefix}``  — full path to the plink fileset basename in the mount
  (e.g. ``/tmp/op-run/mnt/golden``); accepts ``--bfile`` consumers.
- ``${bed}``, ``${bim}``, ``${fam}`` — full paths to the individual files.
- ``${mnt}`` — mount directory.
- ``${out}`` — output prefix in the per-op runtime dir.
- ``${runner}`` — full path to ``scripts/<name>`` for library wrappers.
- ``${aux:NAME}`` — full path to the aux file produced by generator
  ``NAME`` (registered in ``run.py``'s ``AUX_GENERATORS``).

``tool`` is the logical tool name; the harness resolves it through the
``_tools/manifest.json`` written by ``install/install.py`` first, then
falls back to ``$PATH``.
"""

from . import (
    admixture,
    bedreader,
    boltlmm,
    flashpca2,
    gcta,
    king,
    plink,
    regenie,
)
from .base import Operation

OPERATIONS: tuple[Operation, ...] = (
    *plink.OPERATIONS,
    *admixture.OPERATIONS,
    *king.OPERATIONS,
    *gcta.OPERATIONS,
    *flashpca2.OPERATIONS,
    *regenie.OPERATIONS,
    *boltlmm.OPERATIONS,
    *bedreader.OPERATIONS,
)


__all__ = ["Operation", "OPERATIONS"]
