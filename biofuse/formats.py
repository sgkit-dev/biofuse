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

import dataclasses
import pathlib
import tempfile
from collections.abc import Callable

from vcztools import bgen as vcztools_bgen
from vcztools import plink as vcztools_plink


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
    """Suffixes of the static sidecar files, in canonical wire order.

    The wire protocol serialises static-file bodies in this order;
    :meth:`build_static_files` must return a dict whose keys equal
    this tuple."""

    build_static_files: Callable
    """Build the static sidecars for ``reader``.

    Returns a dict ``{suffix: bytes}`` whose keys equal
    :attr:`static_suffixes`."""

    encoder_factory: Callable
    """Construct one fresh encoder for one server connection."""


def _build_plink_static(reader) -> dict[str, bytes]:
    return {
        ".bim": vcztools_plink.generate_bim(reader).encode("utf-8"),
        ".fam": vcztools_plink.generate_fam(reader).encode("utf-8"),
    }


def _build_bgen_static(reader) -> dict[str, bytes]:
    sample = vcztools_bgen.generate_sample(reader).encode("utf-8")
    # ``write_bgen_index`` takes a filesystem path. Materialise the .bgi
    # into a TemporaryDirectory, read the bytes back, then let the
    # context manager clean up. The encoder used to harvest
    # ``variant_offsets`` is I/O-free in ``__init__`` and is discarded
    # once the offsets are read.
    with vcztools_bgen.BgenEncoder(reader) as encoder:
        variant_offsets = encoder.variant_offsets
    with tempfile.TemporaryDirectory(prefix="biofuse-bgen-") as tmp_dir:
        bgi_path = pathlib.Path(tmp_dir) / "index.bgen.bgi"
        vcztools_bgen.write_bgen_index(reader, str(bgi_path), variant_offsets)
        bgi = bgi_path.read_bytes()
    return {".sample": sample, ".bgen.bgi": bgi}


def _plink_encoder_factory(reader):
    return vcztools_plink.BedEncoder(reader)


def _bgen_encoder_factory(reader):
    return vcztools_bgen.BgenEncoder(reader)


PLINK_SPEC = FormatSpec(
    name="plink",
    streaming_suffix=".bed",
    streaming_kind="bed",
    static_suffixes=(".bim", ".fam"),
    build_static_files=_build_plink_static,
    encoder_factory=_plink_encoder_factory,
)


BGEN_SPEC = FormatSpec(
    name="bgen",
    streaming_suffix=".bgen",
    streaming_kind="bgen",
    static_suffixes=(".sample", ".bgen.bgi"),
    build_static_files=_build_bgen_static,
    encoder_factory=_bgen_encoder_factory,
)


SPECS: dict[str, FormatSpec] = {
    PLINK_SPEC.name: PLINK_SPEC,
    BGEN_SPEC.name: BGEN_SPEC,
}
