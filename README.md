# biofuse

Read-only views of VCF Zarr (VCZ) data in standard bioinformatics file formats
via a FUSE filesystem. Currently supported views:

- **PLINK 1.9 binary** (`.bed` / `.bim` / `.fam`) — via `mount-plink`.
- **Oxford BGEN** (`.bgen` / `.sample` / `.bgen.bgi`) — via `mount-bgen`.

## Status

Pre-release. The mount serves the streaming file (`.bed` / `.bgen`) on
demand via a worker subprocess running the matching
[`vcztools`](https://github.com/sgkit-dev/vcztools) encoder
(`BedEncoder` / `BgenEncoder`); the static sidecars are computed once
at mount time and held in the worker's memory.

The mounted PLINK view supports the access patterns of `plink1.9` and
`plink2` for typical analysis commands (`--freq`, `--missing`,
`--hardy`, etc.) — see `tests/test_plink_apps.py` for the verified set.
The mounted BGEN view always uses zlib level 0 (stored, fixed-size
blocks) for O(1) random access; `bgenix` / `qctool` parity checks live
in `tests/test_bgen_apps.py`.

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

### `mount-plink`

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
  are inherited from `vcztools view-plink`. Run `biofuse mount-plink --help`
  or see `vcztools view-plink --help` for the full reference.

Example:

```bash
mkdir /tmp/plink-mnt
biofuse mount-plink ./sample.vcz /tmp/plink-mnt &
plink1.9 --bfile /tmp/plink-mnt/sample --freq --out ./out
fusermount3 -u /tmp/plink-mnt
```

### `mount-bgen`

```bash
biofuse mount-bgen path/to/sample.vcz /mount/dir
```

Mounts a read-only directory at `/mount/dir` containing
`sample.bgen`, `sample.sample`, `sample.bgen.bgi`. The `.bgen` payload
uses zlib level 0 (stored, fixed-size variant blocks) so byte-range
random access is O(1); downstream tools (bgenix, qctool, REGENIE,
SAIGE, BOLT-LMM, plink2 `--bgen`) consume the mount unchanged. The
`.bgen.bgi` SQLite sidecar is generated once at mount time and held in
the worker's memory alongside `.sample`.

Options mirror `mount-plink`: `--basename`, `--access-log`, and the
shared bcftools-style filter / backend / log set inherited from
`vcztools view-bgen`. Run `biofuse mount-bgen --help` or see
`vcztools view-bgen --help` for the full reference.

Example:

```bash
mkdir /tmp/bgen-mnt
biofuse mount-bgen ./sample.vcz /tmp/bgen-mnt &
bgenix -g /tmp/bgen-mnt/sample.bgen -list
fusermount3 -u /tmp/bgen-mnt
```

## Development

```bash
uv sync --group dev
uv run pytest                          # full suite
uv run pytest tests/test_encoder_ops.py  # one module
uv run prek install                    # install git pre-commit hook (one-off)
uv run --only-group=lint prek -c prek.toml run --all-files
```

## Licence

Apache 2.0. See `LICENSE`.
