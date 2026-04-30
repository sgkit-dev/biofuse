"""Materialise a PLINK fileset for a VCZ into a temporary directory.

Phase 1 strategy: rather than streaming bytes from VCZ on demand through FUSE,
biofuse calls the existing ``vcztools.plink.write_plink`` to produce the full
``.bed/.bim/.fam`` fileset on disk, then exposes that directory through a
passthrough FUSE view. This module owns the temporary directory lifecycle.

A future phase will replace this with a streaming source backed by a public
vcztools iterator.
"""

import logging
import pathlib
import shutil
import tempfile

from vcztools import cli as vcztools_cli
from vcztools import plink as vcztools_plink

logger = logging.getLogger(__name__)


class PlinkSource:
    """Context manager that produces a directory of PLINK files.

    On entry, opens the VCZ at ``vcz_path``, runs ``write_plink`` into a
    fresh temporary directory, and returns the directory path. On exit,
    the temporary directory is removed.
    """

    def __init__(
        self,
        vcz_path: str | pathlib.Path,
        *,
        basename: str | None = None,
        backend_storage: str | None = None,
        storage_options: dict | None = None,
    ) -> None:
        self._vcz_path = vcz_path
        self._basename = (
            basename if basename is not None else _default_basename(vcz_path)
        )
        self._backend_storage = backend_storage
        self._storage_options = storage_options
        self._tmpdir: pathlib.Path | None = None

    @property
    def basename(self) -> str:
        return self._basename

    def open(self) -> pathlib.Path:
        """Materialise the fileset and return the backing directory path."""
        if self._tmpdir is not None:
            raise RuntimeError("PlinkSource is already open")
        tmp = pathlib.Path(tempfile.mkdtemp(prefix="biofuse-plink-"))
        try:
            reader = vcztools_cli.make_reader(
                str(self._vcz_path),
                backend_storage=self._backend_storage,
                storage_options=self._storage_options,
            )
            out_prefix = tmp / self._basename
            logger.info(
                "materialising plink fileset for %s at %s",
                self._vcz_path,
                out_prefix,
            )
            vcztools_plink.write_plink(reader, out_prefix)
        except BaseException:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        self._tmpdir = tmp
        return tmp

    def close(self) -> None:
        if self._tmpdir is None:
            return
        shutil.rmtree(self._tmpdir, ignore_errors=True)
        self._tmpdir = None

    def __enter__(self) -> pathlib.Path:
        return self.open()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def _default_basename(vcz_path: str | pathlib.Path) -> str:
    p = pathlib.Path(vcz_path)
    name = p.name
    while True:
        stem = pathlib.Path(name).stem
        if stem == name:
            return stem
        name = stem
