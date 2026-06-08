[![CI](https://github.com/sgkit-dev/biofuse/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/sgkit-dev/biofuse/actions/workflows/ci.yml)
[![PyPI Downloads](https://static.pepy.tech/badge/biofuse)](https://pepy.tech/projects/biofuse)

# biofuse

Read-only views of VCF Zarr (VCZ) data in standard bioinformatics file formats
via a FUSE filesystem. Currently supported views:

- **PLINK 1.9 binary** (`.bed` / `.bim` / `.fam`) — via `mount-plink`.
- **Oxford BGEN** (`.bgen` / `.sample` / `.bgen.bgi`) — via `mount-bgen`.

The streaming file (`.bed` / `.bgen`) is generated on demand using the
matching [`vcztools`](https://github.com/sgkit-dev/vcztools) encoder; the
static sidecars are computed once at mount time.

## Performance and access patterns

biofuse is optimised for **linear, sequential reads** — the access pattern
used by the majority of downstream tools, which stream variants front-to-back.
The streaming `.bed` / `.bgen` file is encoded on demand as the consumer reads
forward, and bytes already produced are buffered, so reading straight through
the file does no redundant work. The mounts are verified against `plink1.9`
and `plink2` (`--bfile`, `--freq`, `--missing`, `--hardy`, …) for PLINK, and
`bgenix`, `qctool`, REGENIE, SAIGE, BOLT-LMM and `plink2 --bgen` for BGEN.

Random and backward access still work, but are slower: seeking backwards or
skipping far ahead can make biofuse re-encode from an earlier point in the
file. The kernel page cache holds bytes that have already been served, so
re-reading a region — and multi-pass tools that scan the file more than once
(e.g. flashpca) — stays cheap once the data is warm.

For BGEN, the `.bgen` payload uses zlib level 0 (stored, fixed-size variant
blocks) together with the `.bgen.bgi` index, so a tool can fetch an individual
variant by byte range without decompressing or re-encoding the rest of the
file — variant-targeted access (e.g. `bgenix -v`) is efficient as well as
whole-file scans.

The sidecar files (`.bim` / `.fam` / `.sample` / `.bgen.bgi`) are computed
once when the mount starts, so reads of them are always fast regardless of
access order.

Because the streaming file is produced on demand, a read that stalls beyond an
internal timeout surfaces as `EIO` rather than blocking indefinitely; in
practice this only appears under pathological random-access load.

## Install

biofuse depends on libfuse 3 system headers (`pyfuse3` builds from source):

```bash
sudo apt-get install -y fuse3 libfuse3-dev pkg-config
```

Then:

```bash
python -m pip install biofuse      # or: uv pip install biofuse
```

### Remote and zipped stores

The `vcz_url` argument and the inherited `--backend-storage` /
`--storage-option` options accept cloud, fsspec, and HTTP stores, plus
`.vcz.zip` files. biofuse depends on bare `vcztools`; to mount cloud-backed
stores install the matching vcztools extra, e.g.
`pip install 'vcztools[obstore]'` or `pip install 'vcztools[icechunk]'`. See
the [vcztools documentation](https://sgkit-dev.github.io/vcztools/) for the
available storage backends.

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
`.bgen.bgi` SQLite sidecar and `.sample` are generated once at mount time.

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

#### Limitations: ploidy

- **Mixed ploidy is not supported by `mount-bgen`.** The fixed-size BGEN
  encoder used for random-access serving requires uniform ploidy across
  every sample and variant in the view. Mounts whose region includes
  mixed-ploidy chromosomes (typically X, Y, MT) open successfully and
  serve `.sample` and `.bgen.bgi`, but the first `.bgen` read will fail
  with `EIO`. Workaround: restrict the view to autosomes at mount time
  (e.g. via the inherited `-r` / `-R` / `-t` / `-T` region filters), or
  use the one-shot `vcztools view-bgen` CLI for full-file conversions
  that include X / Y / MT — `view-bgen` uses the streaming
  variable-size encoder which handles mixed ploidy correctly.
- **Pure haploid VCZ is supported by `mount-bgen`** (the encoder emits a
  uniform-haploid BGEN payload).
- **`mount-plink` is diploid-only.** Pure haploid VCZ inputs (e.g.
  mitochondrial-only stores) are rejected by the underlying encoder
  with `EIO` on the first `.bed` read. Mixed-ploidy VCZ inputs serve
  successfully, but haploid samples are encoded as homozygous for the
  called allele — this matches the PLINK 1 BED format, which has no
  haploid representation.

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
