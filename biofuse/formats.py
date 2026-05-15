"""Format specs for the encoder-server stack.

A :class:`FormatSpec` bundles everything the format-agnostic
``encoder_*`` modules need to serve one output format:

- the suffix of the streaming file (``.bed`` / ``.bgen``),
- a callable returning the static-sidecar suffixes the parent should
  expect for a given options dataclass,
- a builder that produces those static bytes from a ``VczReader`` plus
  options,
- a factory that constructs one :class:`vcztools.BedEncoder` /
  :class:`vcztools.BgenEncoder` per server-side connection, also
  parameterised by the options dataclass.

Both :class:`vcztools.BedEncoder` and :class:`vcztools.BgenEncoder`
extend :class:`vcztools.format_encoder.FormatEncoder`, so they share
the duck-typed ``read(off, size)`` + ``.total_size`` + context-manager
contract — only the static-file shape and the encoder class differ
between PLINK and BGEN.

The set of static sidecars served by a mount is a function of the
``ViewPlinkOptions`` / ``ViewBgenOptions`` dataclass: ``--no-bim`` /
``--no-fam`` suppress the corresponding PLINK sidecars, and
``--no-sample-file`` / ``--no-bgi`` suppress the BGEN sidecars.
``--no-header-samples`` flips ``embed_header_samples=False`` on the
``BgenEncoder`` so the per-connection ``.bgen`` stream omits the
sample identifiers from its header block.

For BGEN the ``.bgen.bgi`` sidecar is a SQLite database that
:func:`vcztools.bgen.write_bgen_index` writes to a filesystem path.
We materialise it to a tempfile at session-init time, read the bytes
back, and hold them in the server process's memory alongside the
``.sample`` text.
"""

import dataclasses
import pathlib
import tempfile
from collections.abc import Callable

import vcztools
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

    static_suffixes: Callable
    """``(opts) -> tuple[str, ...]``: suffixes of the static sidecar files
    the mount will expose for the given options, in canonical wire order.

    The wire protocol serialises static-file bodies in this order;
    :meth:`build_static_files` must return a dict whose keys equal
    the tuple returned here for the same ``opts``."""

    build_static_files: Callable
    """``(reader, opts) -> dict[str, bytes]``: build the static sidecars
    for ``reader`` under ``opts``. Returns a dict whose keys equal
    :meth:`static_suffixes` ``(opts)``."""

    encoder_factory: Callable
    """``(reader, opts) -> FormatEncoder``: construct one fresh encoder
    for one server connection, parameterised by ``opts``."""


def _plink_static_suffixes(opts) -> tuple[str, ...]:
    suffixes = []
    if not opts.no_bim:
        suffixes.append(".bim")
    if not opts.no_fam:
        suffixes.append(".fam")
    return tuple(suffixes)


def _build_plink_static(reader, opts) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    if not opts.no_bim:
        out[".bim"] = vcztools_plink.generate_bim(reader).encode("utf-8")
    if not opts.no_fam:
        out[".fam"] = vcztools_plink.generate_fam(reader).encode("utf-8")
    return out


def _bgen_static_suffixes(opts) -> tuple[str, ...]:
    suffixes = []
    if not opts.no_sample_file:
        suffixes.append(".sample")
    if not opts.no_bgi:
        suffixes.append(".bgen.bgi")
    return tuple(suffixes)


def _build_bgen_static(reader, opts) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    if not opts.no_sample_file:
        out[".sample"] = vcztools_bgen.generate_sample(reader).encode("utf-8")
    if not opts.no_bgi:
        # ``write_bgen_index`` takes a filesystem path. Materialise the
        # .bgi into a TemporaryDirectory, read the bytes back, then let
        # the context manager clean up. The encoder used to harvest
        # ``variant_offsets`` is I/O-free in ``__init__`` and is
        # discarded once the offsets are read.
        with vcztools.BgenEncoder(reader) as encoder:
            variant_offsets = encoder.variant_offsets
        with tempfile.TemporaryDirectory(prefix="biofuse-bgen-") as tmp_dir:
            bgi_path = pathlib.Path(tmp_dir) / "index.bgen.bgi"
            vcztools_bgen.write_bgen_index(reader, str(bgi_path), variant_offsets)
            out[".bgen.bgi"] = bgi_path.read_bytes()
    return out


def _plink_encoder_factory(reader, opts):
    return vcztools.BedEncoder(reader)


def _bgen_encoder_factory(reader, opts):
    return vcztools.BgenEncoder(reader, embed_header_samples=not opts.no_header_samples)


PLINK_SPEC = FormatSpec(
    name="plink",
    streaming_suffix=".bed",
    streaming_kind="bed",
    static_suffixes=_plink_static_suffixes,
    build_static_files=_build_plink_static,
    encoder_factory=_plink_encoder_factory,
)


BGEN_SPEC = FormatSpec(
    name="bgen",
    streaming_suffix=".bgen",
    streaming_kind="bgen",
    static_suffixes=_bgen_static_suffixes,
    build_static_files=_build_bgen_static,
    encoder_factory=_bgen_encoder_factory,
)


SPECS: dict[str, FormatSpec] = {
    PLINK_SPEC.name: PLINK_SPEC,
    BGEN_SPEC.name: BGEN_SPEC,
}
