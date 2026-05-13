"""Format specs for the encoder-server stack.

A :class:`FormatSpec` bundles everything the format-agnostic
``encoder_*`` modules need to serve one output format:

- the suffix of the streaming file (``.bed`` / ``.bgen``),
- the suffixes of the static sidecar files served from cached bytes,
- a builder that produces those static bytes from a ``VczReader``,
- a factory that constructs one :class:`~vcztools.format_encoder.FormatEncoder`
  per server-side connection.

Both :class:`vcztools.plink.BedEncoder` and
:class:`vcztools.bgen.BgenEncoder` extend
:class:`vcztools.format_encoder.FormatEncoder`, so they share the
duck-typed ``read(off, size)`` + ``.total_size`` + context-manager
contract — only the static-file shape and the encoder class differ
between PLINK and BGEN.

For BGEN the ``.bgen.bgi`` sidecar is a SQLite database that
:func:`vcztools.bgen.write_bgen_index` writes to a filesystem path.
We materialise it to a tempfile at session-init time, read the bytes
back, and hold them in the server process's memory alongside the
``.sample`` text. The bytes scale with the variant count
(~50 B/variant); for biobank-scale stores the in-RAM hold is bounded
but non-trivial, and is a candidate to swap for an mmap'd server-side
tempdir in a later phase.
"""

import contextlib
import dataclasses
import os
import pathlib
import tempfile
from collections.abc import Callable

from vcztools import bgen as vcztools_bgen
from vcztools import format_encoder as vcztools_format_encoder
from vcztools import plink as vcztools_plink
from vcztools import retrieval as vcztools_retrieval


@dataclasses.dataclass(frozen=True)
class FormatSpec:
    """One output format the encoder-server stack can serve."""

    name: str
    """Short identifier used in CLI / log lines (``"plink"`` / ``"bgen"``)."""

    streaming_suffix: str
    """File suffix of the streaming file served via per-fh encoder."""

    streaming_kind: str
    """``EncoderOps`` dispatch key for the streaming file."""

    static_suffixes: tuple[str, ...]
    """Suffixes of the static sidecar files, in payload order."""

    build_static_bytes: Callable[[vcztools_retrieval.VczReader], list[bytes]]
    """Build the static sidecars for ``reader``.

    Returns a list parallel to :attr:`static_suffixes`."""

    encoder_factory: Callable[
        [vcztools_retrieval.VczReader],
        contextlib.AbstractContextManager[vcztools_format_encoder.FormatEncoder],
    ]
    """Construct one fresh encoder for one server connection."""


def _build_plink_static(reader: vcztools_retrieval.VczReader) -> list[bytes]:
    bim = vcztools_plink.generate_bim(reader).encode("utf-8")
    fam = vcztools_plink.generate_fam(reader).encode("utf-8")
    return [bim, fam]


def _build_bgen_static(reader: vcztools_retrieval.VczReader) -> list[bytes]:
    sample = vcztools_bgen.generate_sample(reader).encode("utf-8")
    # ``write_bgen_index`` takes a filesystem path. Materialise the .bgi
    # to a tempfile, read the bytes back, then unlink. The encoder used
    # to harvest ``variant_offsets`` is I/O-free in ``__init__`` and is
    # discarded once the offsets are read.
    with vcztools_bgen.BgenEncoder(reader) as encoder:
        variant_offsets = encoder.variant_offsets
    fd, tmp_path_str = tempfile.mkstemp(suffix=".bgi", prefix="biofuse-bgen-")
    os.close(fd)
    tmp_path = pathlib.Path(tmp_path_str)
    try:
        vcztools_bgen.write_bgen_index(reader, str(tmp_path), variant_offsets)
        bgi = tmp_path.read_bytes()
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass
    return [sample, bgi]


def _plink_encoder_factory(
    reader: vcztools_retrieval.VczReader,
) -> contextlib.AbstractContextManager[vcztools_format_encoder.FormatEncoder]:
    return vcztools_plink.BedEncoder(reader)


def _bgen_encoder_factory(
    reader: vcztools_retrieval.VczReader,
) -> contextlib.AbstractContextManager[vcztools_format_encoder.FormatEncoder]:
    return vcztools_bgen.BgenEncoder(reader)


PLINK_SPEC = FormatSpec(
    name="plink",
    streaming_suffix=".bed",
    streaming_kind="bed",
    static_suffixes=(".bim", ".fam"),
    build_static_bytes=_build_plink_static,
    encoder_factory=_plink_encoder_factory,
)


BGEN_SPEC = FormatSpec(
    name="bgen",
    streaming_suffix=".bgen",
    streaming_kind="bgen",
    static_suffixes=(".sample", ".bgen.bgi"),
    build_static_bytes=_build_bgen_static,
    encoder_factory=_bgen_encoder_factory,
)


SPECS: dict[str, FormatSpec] = {
    PLINK_SPEC.name: PLINK_SPEC,
    BGEN_SPEC.name: BGEN_SPEC,
}
