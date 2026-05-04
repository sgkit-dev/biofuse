"""Per-tool download registry for the IO-pattern study.

Each ``ToolSpec`` describes how to fetch one external tool from its
upstream distribution and place an executable at
``_tools/<name>/<exe_name>``.

Distribution shapes ``install.py`` knows how to handle:

- ``tar.gz`` — gzipped tarball; ``archive_member`` is a glob matched
  against the archive's contents to identify the binary to keep.
- ``zip`` — same, for zip archives.
- ``gz`` — single gzip-compressed file (the binary itself).
- ``raw`` — bare binary served as-is.

``sha256`` is optional. If unset, the installer records the observed
hash on first install; pin it later for byte-exact reproducibility.
``smoke_argv`` is a short argv (relative to the installed binary)
that should exit 0 — used as a post-install sanity check.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ToolSpec:
    name: str
    url: str
    archive: str  # one of: tar.gz, tar.bz2, zip, gz, raw
    exe_name: str
    archive_member: str | None = None
    sha256: str | None = None
    smoke_argv: tuple[str, ...] = ()
    # Extra archive members to extract alongside the main exe. Glob
    # pattern → output filename relative to ``_tools/<name>/``. Used by
    # tools that ship private shared libraries the binary expects to
    # find on its dynamic loader path (e.g. BOLT-LMM bundles libiomp5).
    extra_members: tuple[tuple[str, str], ...] = ()


REGISTRY: tuple[ToolSpec, ...] = (
    ToolSpec(
        name="admixture",
        url="https://dalexander.github.io/admixture/binaries/admixture_linux-1.3.0.tar.gz",
        archive="tar.gz",
        archive_member="*/admixture",
        exe_name="admixture",
        smoke_argv=("--help",),
    ),
    ToolSpec(
        name="king",
        url="https://www.kingrelatedness.com/Linux-king.tar.gz",
        archive="tar.gz",
        archive_member="king",
        exe_name="king",
        smoke_argv=("--help",),
    ),
    ToolSpec(
        name="gcta",
        url="https://github.com/jianyangqt/gcta/releases/download/v1.94.1/gcta-1.94.1-linux-x86_64-static",
        archive="raw",
        exe_name="gcta",
        smoke_argv=("--version",),
    ),
    ToolSpec(
        name="flashpca",
        url="https://github.com/gabraham/flashpca/releases/download/v2.0/flashpca_x86-64.gz",
        archive="gz",
        exe_name="flashpca",
        smoke_argv=("--help",),
    ),
    ToolSpec(
        name="regenie",
        url="https://github.com/rgcgithub/regenie/releases/download/v4.1/regenie_v4.1.gz_x86_64_Linux.zip",
        archive="zip",
        archive_member="regenie_*",
        exe_name="regenie",
        smoke_argv=("--help",),
    ),
    ToolSpec(
        name="bolt",
        url="https://storage.googleapis.com/broad-alkesgroup-public/BOLT-LMM/downloads/BOLT-LMM_v2.5.tar.gz",
        archive="tar.gz",
        archive_member="*/bolt",
        exe_name="bolt",
        smoke_argv=("--help",),
        # bolt is dynamically linked against Intel's libiomp5; the
        # tarball ships its own copy under lib/. The harness sets
        # LD_LIBRARY_PATH to ``_tools/bolt/lib`` for ops that invoke it.
        extra_members=(("*/lib/libiomp5.so", "lib/libiomp5.so"),),
    ),
)
