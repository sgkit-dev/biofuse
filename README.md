# biofuse

Read-only views of VCF Zarr (VCZ) data in standard bioinformatics file formats
via a FUSE filesystem. The first supported view is **PLINK 1.9 binary**
(`.bed` / `.bim` / `.fam`).

## Status

Phase 1 / pre-release. The current implementation materialises a complete
PLINK fileset to a temporary directory at mount time using
[`vcztools.plink.write_plink`](https://github.com/sgkit-dev/vcztools) and
serves the directory through FUSE. A future phase will replace the
materialisation step with a streaming source.

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
- `--backend-storage {fsspec,obstore,icechunk}` — backend for remote VCZ URLs.
- `--access-log PATH` — record every read as a JSONL row to PATH (useful for
  characterising consumer access patterns).
- `-v` / `-vv` — increase logging verbosity.

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
uv run pytest tests/test_passthrough_view.py  # one module
uv run prek install       # install git pre-commit hook (one-off)
uv run --only-group=lint prek -c prek.toml run --all-files
```

## Roadmap

- **Phase 2 — streaming plink source.** The materialisation step in
  phase 1 doesn't scale to large remote VCZ stores. The replacement
  is a streaming source in `vcztools` that biofuse calls byte-range
  reads against. The consumer-driven requirements spec lives at
  [`specs/vcztools_streaming_plink.md`](specs/vcztools_streaming_plink.md);
  the empirical basis is the IO study at
  [`experiments/io-study/report.md`](experiments/io-study/report.md).

## Licence

Apache 2.0. See `LICENSE`.
