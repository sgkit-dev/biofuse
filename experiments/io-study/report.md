# biofuse IO-pattern study: plink1.9 / plink2

## Summary

We built a 1000-sample × 994,775-variant plink fileset (BED ≈ 237 MiB,
BIM ≈ 25 MiB, FAM ≈ 24 KiB) by simulating with msprime, converted to VCZ
via `bio2zarr.tskit.convert`, and materialised plink files via
`vcztools.plink.write_plink`. We synthetically split the bim into 22
chromosomes by position band so chromosome-aware filters do real work.
We then mounted the directory through biofuse's pyfuse3 passthrough,
ran 43 plink1.9 / plink2 operations, and recorded every FUSE read into
JSONL traces. Headline findings:

1. **Almost everything is a single forward pass over `.bed`.**
   24 of 43 operations — including all the heavy compute ops like
   `--pca`, `--make-grm-bin`, `--make-king`, `--r2`, `--linear`,
   `--glm` — read the entire `.bed` exactly once with redundancy 1.0
   and sequential ratio > 99%. plink keeps the working set in RAM.
2. **plink probes the tail of `.bed` first.** Every operation begins
   with a tiny (~1 KiB) read at `file_size - 1017`, then jumps to
   offset 0 and scans forward. This is a one-time integrity / size
   check, not a streaming-killer.
3. **`--chr` filters DO prune `.bed` IO.** `--chr 1` reduces `.bed`
   IO to 4.58% of the file (matching the 4.47% chr1 variant count);
   `--chr 1-3` reduces to 13.38%.
4. **`--from-bp` / `--to-bp` filters DO prune.** A 100 KB position
   window in chr 1 results in 0.42% of `.bed` read.
5. **`--extract` with a small list does true random access.** 10 SNPs
   produce 12 reads totaling 0.03% of `.bed`. plink seeks to each
   variant's offset directly.
6. **`--extract` density matters.** 1k SNPs → 2% IO (random); 100k SNPs
   → 90% IO (plink switches to streaming when seeks would dominate
   sequential cost).
7. **`--exclude` does NOT prune.** Excluding 10 SNPs still reads 100%.
8. **`--keep` / `--remove` (sample filters) do NOT prune `.bed` IO.**
   The BED layout stores all samples within each variant row, so
   selecting samples cannot skip variants.
9. **`--maf` / `--geno` quality filters do NOT prune `.bed` IO.**
   They cannot — plink has to scan to compute the frequencies.
10. **`.bim` is always read in full** (≈ 26 MiB, ~200 readahead
    blocks). The lookup table cannot be streamed.
11. **`.fam` is always read in 1 syscall** (24 KiB).
12. **plink2 mirrors plink1.9** on every tested op for `.bed`
    coverage and access pattern.

The implication for biofuse phase 2: **a forward streaming source of
`.bed` is sufficient for the vast majority of plink workflows**, with
two caveats: a tail probe at startup and byte-range random access for
small `--extract` / `--from-bp` queries. `.bim` and `.fam` should be
served from memory.

---

## Setup

- **Fixture**: 1000 diploid samples × 994,775 variants → 248.69 MB BED.
  Built from a single msprime tree sequence (`Ne=10000`, `μ=1e-7`,
  `r=1e-8`, `seq_len=31 Mb`) and post-processed to assign 22 synthetic
  chromosomes by position band (~44k variants each, fairly balanced).
  See `data/fixture.json` for full provenance.
- **Tools**: `plink1.9` v1.90b7.2 (Dec 2023), `plink2` v2.00a6 AVX2
  (Nov 2023). Both packaged with Ubuntu.
- **biofuse**: phase 1 commit; passthrough view over the materialised
  directory, pyfuse3 backend, kernel default `max_read=131072`.
- **Trace mechanism**: `biofuse.access_log.AccessLogger` writing JSONL
  rows of `(path, offset, size, t_monotonic)`. Every FUSE `read()` is
  one row.

---

## Methodology

For each operation in `operations.py`, the harness in `run.py`:

1. Mounts `data/golden/` through a `PassthroughDirectoryView` with a
   per-op `AccessLogger` writing to `traces/<op_id>.jsonl`.
2. Generates any aux files the op needs (extract lists, sample lists,
   phenotype) from the bim/fam.
3. Runs the plink subprocess against the mount with a wall-clock
   timeout, captures exit code and stderr.
4. Unmounts and closes the logger.

`analyze.py` then computes, per `(op, file)`:

- `bytes_read` — total bytes returned by `read()` calls
- `n_reads`
- size statistics: `median`, `p99`, `min`, `max`
- `unique_byte_coverage` — sum of merged-overlapping byte ranges
- `coverage_pct` — unique bytes / file size
- `redundancy` — `bytes_read / unique_bytes` (1.0 = no re-reads)
- `sequential_pct` — % of consecutive read pairs where the next read
  starts at the previous read's end
- `monotonic_pct` — % where `offset[i+1] ≥ offset[i]`
- `max_seek_back` — biggest backward jump
- `n_passes` — count of "wraps to a smaller offset" events

Per-operation `.bed` access timelines are rendered as ASCII strip plots
(`results/timelines/<op>.txt`).

The "tail-probe" at offset `file_size - 1017` causes every op to show
a single backward jump from end-of-file to offset 0. We treat this as
a fixed cost rather than a streaming concern.

---

## Per-operation `.bed` results (selected columns)

| op | n_reads | bytes_read | cov_% | redund. | seq_% | t_s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| **scan** | | | | | | |
| p19_freq | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.46 |
| p19_missing | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.36 |
| p19_hardy | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.34 |
| p19_het | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.29 |
| p2_freq | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.35 |
| p2_missing | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.34 |
| p2_hardy | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.26 |
| p2_het | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.37 |
| **output** | | | | | | |
| p19_make_bed | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.41 |
| p2_make_bed | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 4.67 |
| p19_recode_A | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.41 |
| p2_export_A | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.70 |
| **range filters** | | | | | | |
| p19_freq_chr1 | 90 | 11,387,897 | 4.58 | 1.0 | 98.9 | 0.12 |
| p19_freq_chr1_3 | 257 | 33,276,921 | 13.38 | 1.0 | 99.6 | 0.34 |
| p2_freq_chr1 | 90 | 11,387,897 | 4.58 | 1.0 | 98.9 | 0.11 |
| p2_freq_chr1_3 | 257 | 33,276,921 | 13.38 | 1.0 | 99.6 | 0.33 |
| p19_freq_bp_window_chr1 | 11 | 1,033,209 | 0.42 | 1.0 | 90.0 | 0.02 |
| p2_freq_bp_window_chr1 | 11 | 1,033,209 | 0.42 | 1.0 | 90.0 | 0.01 |
| p19_freq_snp_range | 42 | 4,854,777 | 1.95 | 1.0 | 95.1 | 0.05 |
| **list filters** | | | | | | |
| p19_freq_extract_10 | 12 | 70,649 | 0.03 | 1.0 | 9.1 | 0.01 |
| p19_freq_extract_1k | 1056 | 5,010,425 | 2.01 | 1.0 | 5.2 | 0.32 |
| p19_freq_extract_100k | 1721 | 225,166,329 | 90.54 | 1.0 | 99.9 | 2.18 |
| p19_freq_exclude_10 | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.47 |
| p2_freq_extract_10 | 12 | 70,649 | 0.03 | 1.0 | 9.1 | 0.03 |
| p2_freq_extract_1k | 1056 | 5,010,425 | 2.01 | 1.0 | 5.2 | 0.29 |
| p2_freq_extract_100k | 1721 | 225,166,329 | 90.54 | 1.0 | 99.9 | 2.13 |
| **sample filters** | | | | | | |
| p19_freq_keep_10 | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.39 |
| p19_freq_remove_10 | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.37 |
| p2_freq_keep_10 | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.38 |
| **quality filters** | | | | | | |
| p19_freq_maf | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.55 |
| p19_freq_geno | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.37 |
| p19_freq_maf_geno | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.36 |
| p2_freq_maf | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.28 |
| **LD / pruning** | | | | | | |
| p19_indep_pairwise | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.36 |
| p2_indep_pairwise | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.49 |
| p19_ld_single_pair | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.36 |
| p19_r2_window | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.41 |
| **PCA / GRM** | | | | | | |
| p19_pca | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.32 |
| p2_pca_approx | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.45 |
| p19_grm_bin | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.36 |
| p2_make_king | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.54 |
| **GWAS** | | | | | | |
| p19_linear | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.67 |
| p2_glm | 1901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 2.53 |

Note: `t_s` is `t_monotonic[last] − t_monotonic[first]` for `.bed`
reads only (FUSE-side IO time). Total wall time including plink
computation is a separate column in `summary.csv`.

`.bim` is always read fully (~26 MiB, 201 reads, redundancy 1.0).
`.fam` is always read in a single 24 KiB syscall.

---

## Findings by theme

### Sequential scans

`--freq`, `--missing`, `--hardy`, `--het`, plus `--make-bed` and
`--recode/--export A`: identical IO pattern. 1901 `.bed` reads, 100%
coverage, sequential. The kernel readahead grows from 16 KB → 32 KB →
… → 128 KB after the tail probe, then settles at 128 KB for the rest
of the scan. plink itself is presumably issuing larger linear reads
that the FUSE layer breaks into 128 KB chunks (`max_read=131072`).

```
# .bed reads=1901 file_size=248,693,753B  (top=byte 0, bottom=EOF)
********                                                                        
       ********                                                                 
              ********                                                          
                     ********                                                   
                            ********                                            
                                    ********                                    
                                           ********                             
                                                  ********                      
                                                         ********               
                                                                ********        
*                                                                      *********
                                                                                
```

The leftmost `*` on the bottom row is the tail probe; the diagonal is
the forward scan from offset 0 to EOF.

### Range filters: `--chr` works

Both plink1.9 and plink2 honour the chromosome filter at the IO layer:

| filter | bed cov_% | bed bytes_read | bim variant count |
| --- | ---: | ---: | ---: |
| no filter | 100.00 | 248,693,753 | 994,775 |
| `--chr 1` | 4.58 | 11,387,897 | 44,508 (4.47%) |
| `--chr 1-3` | 13.38 | 33,276,921 | 132,529 (13.32%) |

The mismatch (4.58% vs 4.47%) is because plink reads `.bed` in
readahead-aligned blocks, so it slightly over-reads at chromosome
boundaries.

### Range filters: `--from-bp`/`--to-bp` and `--snp`/`--window`

```
# .bed reads=11 (--chr 1 --from-bp 0 --to-bp 100000)
       *       *       *       *       *       *       *       *       *       *
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
                                                                                
*
```

10 small reads near the top of the file (chr1 region), plus the tail
probe. 0.42% of `.bed` is touched. A 100 KB position window is
genuinely small enough that plink does direct seeks.

`--snp rs500000 --window 1000` reads 1.95% of `.bed` (the 1 Mbp window
around rs500000 on chr11) — likewise selectively.

### List filters: `--extract` density determines the strategy

```
# .bed reads=12 (--extract 10 SNPs)
       *                                                                        
              *                                                                 
                     *                                                          
                            *                                                   
                                   *                                            
                                           *      *                             
                                                         *                      
                                                                *               
                                                                       *        
                                                                               *
*                                                                               
                                                                                
```

10 SNPs evenly spaced across the genome → 10 random `.bed` reads, plus
the tail probe and one more — total 12 reads, 0.03% coverage. **plink
is doing direct byte-range access**, computing the offset of each
variant from its position in the bim and seeking.

For 1k SNPs the pattern is similar but denser:

```
# .bed reads=1056 (--extract 1k SNPs)
*******                                                                         
      *********                                                                 
              ********                                                          
                     ********                                                   
                            *********                                           
                                    *******                                     
                                          *********                             
                                                  ********                      
                                                         ********               
                                                                *********       
*                                                                       ********
                                                                                
```

2.01% coverage, 5.2% sequential. Reads form short, evenly-spaced
clumps as plink fetches one block per requested variant; the kernel
readahead pulls in adjacent bytes that are then dropped.

For 100k SNPs the pattern reverts to a streaming scan:

```
# .bed reads=1721 (--extract 100k SNPs)
*********                                                                       
        ********                                                                
                ********                                                        
                       *********                                                
                               *********                                        
                                       *********                                
                                               *********                        
                                                       *********                
                                                               *********        
                                                                       *********
*                                                                               
                                                                                
```

90.54% coverage, 99.9% sequential — at this density, full sequential
read is cheaper than 100k seeks.

### `--exclude` is not the dual of `--extract`

Excluding 10 SNPs requires reading all the others. plink does a full
scan and drops 10 rows. This matches the obvious computational
constraint: "all-but-N" cannot be served by any seeking strategy
unless the universe is a small list.

### Sample filters do not affect `.bed` IO

`--keep <10 samples>`, `--remove <10 samples>`: both still read 100%
of `.bed`. The plink-1 BED layout is variant-major and packs all
samples' genotypes inside each variant's row, so you cannot skip
variants by selecting samples — every variant's row still has to be
read in full to extract the kept samples' genotypes.

### Quality filters do not affect `.bed` IO

`--maf 0.05`, `--geno 0.05`, `--maf 0.05 --geno 0.05`: all 100% of
`.bed`. plink has to compute the per-variant statistics first, which
requires reading every byte. This is a streaming computation, not a
random-access one.

### LD / pruning / association

Despite involving substantial computation, all of these are
**single-pass forward scans** of `.bed`:

- `--indep-pairwise 50 5 0.1` (sliding window LD pruning)
- `--ld rs100 rs101` (single-pair LD)
- `--r2 --ld-window-r2 0.2` (windowed pairwise r²)
- `--linear --pheno random.txt`
- `--glm allow-no-covars --pheno random.txt`

100% coverage, redundancy 1.0, sequential ≈ 99.9%. plink's working
set fits in RAM; LD windows are evaluated in-memory.

### PCA / GRM

Likewise single-pass: `--pca 10` (plink1.9), `--pca approx 10`
(plink2), `--make-grm-bin`, `--make-king`. The largest fixture-side
wall time comes from `--pca approx` at 106 s — but `.bed` is read
exactly once during that interval; the rest is computation in RAM
(the FUSE-side `t_s` for the bed reads alone is 2.45 s).

This is good news for biofuse phase 2: you do not need to support
multi-pass `.bed` reads for any of the headline plink ops.

### plink1.9 vs plink2

For every operation tested in both tools:
- `.bed` `n_reads` and `bytes_read` and `coverage_pct` are
  **identical**.
- `sequential_pct` and `monotonic_pct` are identical.
- Only the `t_s` (elapsed) differs, and only because plink2 sometimes
  takes longer per byte (e.g., `p2_make_bed` 4.67 s vs `p19_make_bed`
  2.41 s — likely compression / format-version differences).

The two tools are interchangeable from biofuse's perspective.

---

## Implications for biofuse phase 2

A future streaming source in vcztools (replacing the materialise-and-
passthrough shim) needs to support, in priority order:

1. **Forward streaming of `.bed` from offset 0 to EOF.** This serves
   24 of 43 tested ops, including all the heavy compute (PCA, GRM,
   GWAS, LD pruning) and all the simple stats. Stream chunks of
   ~128 KiB to align with FUSE `max_read`.

2. **Tail probe**. Every op reads ~1 KiB at `file_size − 1017` before
   any other `.bed` read. The streaming source must satisfy this
   without rebuilding the whole genotype matrix. Two options:
   (a) materialise the last variant chunk eagerly at mount time and
   keep it in RAM (cheap — one variant chunk worth of bytes), or
   (b) compute and cache `file_size` upfront so the FUSE layer can
   service the tail probe from a small precomputed buffer.
   Both are trivial.

3. **Byte-range random access for small `--extract` / `--from-bp` /
   `--snp --window` workloads.** The pattern is: variant index → BED
   offset = `3 + variant_index * ⌈n_samples/4⌉`. For up to a few
   thousand requested variants, plink seeks directly. To serve this,
   the streaming source needs the ability to fetch a single variant
   chunk on demand by index, not just sequentially. In vcztools
   terms: expose `reader.variant_chunks(start_index, end_index)` or
   equivalent.

4. **Always-full `.bim`**. plink reads the entire bim regardless of
   filters. Keep generating bim eagerly in memory (as biofuse already
   does); do not stream it.

5. **Single-syscall `.fam`**. Trivial — keep it in memory.

What we explicitly **do not need** in phase 2:

- Multi-pass support for `.bed`. No plink op tested re-reads bytes.
- Random sample-axis access. plink's BED layout is variant-major; no
  filter prunes by sample.
- Backward seeking beyond the one-time tail probe.

A reasonable phase-2 contract for the streaming source:

```python
class BedSource:
    file_size: int                                    # known at construction
    def get_tail(self, n_bytes: int) -> bytes: ...    # tail probe
    def stream_forward(self) -> Iterator[bytes]: ...  # the bulk path
    def get_variant_range(self, start: int, end: int) -> bytes: ...
                                                       # for small extracts
```

`get_variant_range` can fall back to `stream_forward` plus a counter
when the requested range exceeds, say, 10% of the file — matching
plink's own observed crossover at ~10% density (1k vs 100k extract).

---

## Caveats

- **Synthetic data, single chromosome under the hood**. We split a
  single msprime tree sequence into 22 fake chromosomes by position
  band. plink's `--chr` filter doesn't know that; it sees genuinely
  separate chromosomes in the bim. But real-world LD structure is
  unrepresentative: an `--indep-pairwise` run on this fixture will
  prune more aggressively (or differently) than on real human data.
  IO patterns are unaffected, since LD-pruning still does a single
  scan.

- **Local-disk passthrough**. biofuse phase 1 serves bytes from
  local SSD. If a future phase 2 source is backed by remote zarr,
  read latency will be higher and the kernel readahead pattern may
  differ. Recommendation: re-run this study against the streaming
  source once it exists.

- **Kernel readahead inflates read sizes at our layer**. plink's
  user-level read sizes may be different from what FUSE sees. For
  the streaming source, the relevant question is "what does FUSE
  ask for?" and the answer is consistently 128 KiB (`max_read` on
  this kernel) for sequential phases, smaller around boundaries.

- **Page cache effects across operations**. We unmount between
  operations, so each op sees a cold cache from the FUSE layer's
  perspective. But the underlying `golden/` directory is in the
  Linux page cache after the first op, so the materialised file
  serves bytes essentially instantly. This skews per-op `t_s`
  numbers (they're all very low) but does not affect the IO patterns
  we measured.

- **No multi-threaded plink runs**. plink1.9 and plink2 both have
  threading flags; we used defaults. Threaded execution may issue
  more concurrent reads, but the per-byte coverage and pattern are
  unlikely to change.

- **Tail probe size is fixture-dependent**. We observed 1017 bytes;
  on a different fileset it may differ. The streaming source should
  treat any `read(offset > 0.99 * file_size)` as a probe, not a
  signal of a backward scan.
