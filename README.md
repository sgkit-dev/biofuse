# biofuse

Read-only views of VCF Zarr (VCZ) data in standard bioinformatics file formats
via a FUSE filesystem. The first supported view is **PLINK 1.9 binary**
(`.bed` / `.bim` / `.fam`).

## Status

Pre-release. The mount serves `.bed` on demand via a worker subprocess
running [`vcztools.BedEncoder`](https://github.com/sgkit-dev/vcztools);
`.bim` and `.fam` are computed once at mount time and held in the
worker's memory.

The mounted view is read-only and supports the access patterns of `plink1.9`
and `plink2` for typical analysis commands (`--freq`, `--missing`, `--hardy`,
etc.) — see `tests/test_plink_apps.py` for the verified set.

## Install

biofuse depends on libfuse 3 system headers when building from source:

```bash
sudo apt-get install -y fuse3 libfuse3-dev pkg-config
```

Then with [`uv`](https://docs.astral.sh/uv/):

```bash
uv sync --group test
```

vcztools is currently consumed as a sibling-directory path dependency
(`../vcztools`); see `pyproject.toml`.

## Usage

```bash
biofuse mount-plink path/to/sample.vcz /mount/dir
```

Mounts a read-only directory at `/mount/dir` containing
`sample.bed`, `sample.bim`, `sample.fam`. The mount runs in the foreground;
press Ctrl-C to unmount.

Options:

- `--basename NAME` — basename for the plink fileset (defaults to the VCZ stem).
- `--access-log PATH` — record every read as a JSONL row to PATH (useful for
  characterising consumer access patterns).
- The bcftools-view-style filter / backend / log options
  (`-r`/`-R`/`-s`/`-S`/`-t`/`-T`/`-i`/`-e`/`-v`/`-V`/`-m`/`-M`,
  `--backend-storage`, `--storage-option`, `--log-level`, `--log-file`)
  are inherited from `vcztools view-bed`. Run `biofuse mount-plink --help`
  or see `vcztools view-bed --help` for the full reference.

Example:

```bash
mkdir /tmp/plink-mnt
biofuse mount-plink ./sample.vcz /tmp/plink-mnt &
plink1.9 --bfile /tmp/plink-mnt/sample --freq --out ./out
fusermount3 -u /tmp/plink-mnt
```

## Development

```bash
uv sync --group dev
uv run pytest             # full suite
uv run pytest tests/test_bed_worker.py  # one module
uv run prek install       # install git pre-commit hook (one-off)
uv run --only-group=lint prek -c prek.toml run --all-files
```

The streaming source spec lives at
[`specs/vcztools_streaming_plink.md`](specs/vcztools_streaming_plink.md).

## Licence

Apache 2.0. See `LICENSE`.
