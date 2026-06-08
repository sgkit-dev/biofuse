# Changelog

## [0.1.0] - 2026-06-08

Initial release of biofuse.

- Read-only FUSE views of VCF Zarr data in standard bioinformatics formats,
  generated on demand using vcztools.
- `mount-plink` — PLINK 1.9 binary view (`.bed`/`.bim`/`.fam`), diploid input.
- `mount-bgen` — Oxford BGEN view (`.bgen`/`.sample`/`.bgen.bgi`) using zlib
  level 0 fixed-size blocks for O(1) random access; haploid supported,
  mixed ploidy not supported by the mount.
- Inherits vcztools view filter / backend / log options on both commands, plus
  `--basename` and `--access-log`.
- Requires vcztools >= 0.2.
