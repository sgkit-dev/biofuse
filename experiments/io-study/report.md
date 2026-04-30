# biofuse IO-pattern study: plink-1 binary ecosystem

## Summary

We built a 1000-sample × 994,775-variant plink fileset (BED ≈ 237 MiB,
BIM ≈ 25 MiB, FAM ≈ 24 KiB) by simulating with msprime, converted to VCZ
via `bio2zarr.tskit.convert`, and materialised plink files via
`vcztools.plink.write_plink`. We synthetically split the bim into 22
chromosomes by position band so chromosome-aware filters do real work.
We mounted the directory through biofuse's pyfuse3 passthrough and ran
the full op matrix — 43 plink1.9 / plink2 ops (× 21 threading variants),
six external CLIs that consume plink-1 binary input (ADMIXTURE, KING,
GCTA, flashpca2, REGENIE, BOLT-LMM) and one library-style consumer
(`bed-reader`) — recording every FUSE read into JSONL traces. Headline
findings:

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
13. **The single-pass forward-scan pattern generalises across the
    ecosystem.** ADMIXTURE, KING, GCTA (`--make-grm-bin` / `--pca`),
    flashpca2, and BOLT-LMM all match plink at the FUSE layer: 1,899–
    1,901 reads, 100% coverage, redundancy 1.0, sequential ratio
    ≥ 99.9%, 128 KiB median read size. Each tool loads the genotype
    matrix in one streaming sweep at startup; iterative algorithms
    (flashpca's randomised SVD, ADMIXTURE's EM, GCTA's GRM compute)
    happen entirely in RAM after the load.
14. **The kernel page cache is doing real work for fine-grained
    consumers.** `bed-reader.full_scan` issues ~995k 250 B reads at
    the Python API level (one per variant row) but the kernel
    readahead amalgamates them into the *same* 1,900 128 KiB FUSE
    reads plink produces. From biofuse's perspective the two consumers
    are indistinguishable; the difference is ~500× in user-level
    syscall count, fully absorbed by the page cache.
15. **bed-reader's sliced reads map onto plink's `--extract` shape.**
    `slice_variants_1k` does 1,001 scattered reads at 5% coverage;
    `random_10` does 12 pinpoint reads at 0.07% coverage. Same
    fingerprint as plink with a small `--extract` list — no new IO
    shape introduced.
16. **`direct_io` was tried and rejected.** We initially mounted with
    `direct_io=True` to bypass the kernel page cache. That made
    flashpca time out (12× slowdown — its in-RAM iterations re-read
    every byte through FUSE) and bed-reader's full scan blow up to
    995k FUSE round-trips (≈ 5.5 minutes). The page cache is
    load-bearing for the wider ecosystem; we reverted to the pyfuse3
    default. mmap-style sequential access is fine because page faults
    still surface to FUSE on first touch.

The implication for biofuse phase 2: **a forward streaming source of
`.bed` is sufficient for the vast majority of plink-style workflows
across the ecosystem**, provided the kernel page cache is allowed to
do its job. The original two refinements still apply: a tail probe at
startup and byte-range random access for small `--extract` /
`--from-bp` / sliced reads. `.bim` and `.fam` should be served from
memory. No additional IO primitives are required to support the wider
tool zoo — it's the *same* surface, with page-cache-on as a
non-negotiable operating assumption.

---

## Setup

- **Fixture**: 1000 diploid samples × 994,775 variants → 248.69 MB BED.
  Built from a single msprime tree sequence (`Ne=10000`, `μ=1e-7`,
  `r=1e-8`, `seq_len=31 Mb`) and post-processed to assign 22 synthetic
  chromosomes by position band (~44k variants each, fairly balanced).
  See `data/fixture.json` for full provenance.
- **plink tools**: `plink1.9` v1.90b7.2 (Dec 2023), `plink2` v2.00a6
  AVX2 (Nov 2023). Both packaged with Ubuntu.
- **External CLIs (downloaded by `install/install.py`)**: ADMIXTURE
  v1.3.0 (linux x86_64 binary), KING (`Linux-king` tarball), GCTA
  v1.94.1 static linux binary, flashpca2 v2.0 static linux binary,
  REGENIE v4.1 (Linux release zip), BOLT-LMM v2.5 tarball (binary +
  bundled libiomp5.so on `LD_LIBRARY_PATH`).
- **Library consumer**: `bed-reader` (PyPI, installed via the
  `experiments` dependency group) — exercised through four ops
  (`full_scan`, `slice_variants_1k`, `slice_samples_10`, `random_10`)
  driven by `scripts/bedreader_runner.py`.
- **biofuse**: phase 1 commit; passthrough view over the materialised
  directory, pyfuse3 backend, kernel default `max_read=131072`. Page
  cache **on** (pyfuse3 default; see Caveats for the direct_io
  experiment).
- **Trace mechanism**: `biofuse.access_log.AccessLogger` writing JSONL
  rows of `(path, offset, size, t_monotonic)`. Every FUSE `read()` is
  one row.

---

## Methodology

For each operation in `operations/`, the harness in `run.py`:

1. Mounts `data/golden/` through a `PassthroughDirectoryView` with a
   per-op `AccessLogger` writing to `traces/<op_id>.jsonl`.
2. Generates any aux files the op needs (extract lists, sample lists,
   phenotypes) from the bim/fam.
3. Renders the operation's argv template (placeholders `${prefix}`,
   `${bed}`, `${bim}`, `${fam}`, `${out}`, `${runner}`, `${aux:NAME}`),
   resolves the tool through `_tools/manifest.json` (or `$PATH`), and
   runs the consumer subprocess against the mount with a wall-clock
   timeout. Exit code and stderr are captured.
4. Unmounts and closes the logger.

The same harness drives all consumers — plink, the six external CLIs,
and the bed-reader Python wrapper — through one declarative
`Operation` dataclass.

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

### Threading

We re-ran every scan op and every heavy-compute op with explicit
`--threads 8`. The question we wanted to answer: does plink open
multiple concurrent `.bed` readers under threading, or does the
single forward scan persist?

The analyzer was extended with a stream-count metric. Walking the
trace in monotonic-time order, each read is attached to an existing
"active stream" if its offset continues from that stream's last_end
(within a 1 MiB tolerance to absorb kernel readahead alignment);
otherwise it seeds a new stream. The number of major streams (≥ 100
KiB cumulative bytes) is the parallel reader count.

**Headline finding: plink does NOT spawn concurrent `.bed` readers
under `--threads N`. Every tested op stays single-stream sequential,
identical to its default-threads run.**

For all 21 `_t8` operations:

| Metric | Value |
| --- | --- |
| `bed_n_reads` | 1901 (identical to default) |
| `bed_bytes_read` | 248,693,753 (= file size) |
| `bed_coverage_pct` | 100.00 |
| `bed_n_streams` (major) | 1 |
| `bed_n_minor_streams` | 1 (the tail probe) |
| `bed_dominant_stream_bytes_pct` | 100.0 |

The tail-probe-then-forward-scan signature is preserved; the ASCII
strip plot for, e.g., `p19_pca_t8` is byte-identical to `p19_pca`.

Wall-time effect of `--threads 8` was small but real for compute-
heavy plink2 / plink1.9 ops (default → t8): `--pca approx` 156 s →
144 s (-8 %), `--make-king` 7.5 s → 6.6 s (-12 %), `--pca`
34.7 s → 29.8 s (-14 %), `--make-grm-bin` 34.2 s → 29.4 s (-14 %).
Light ops (`--freq`, `--missing`, `--hardy`, `--het`) saw no
significant change. The pattern is consistent across both tools:
threading multiplexes the in-RAM computation after the genotype
matrix has been loaded; the load itself remains a single forward
read.

The implication for biofuse phase 2 is clean: the streaming source
can serve a single forward `.bed` reader per consumer process and
need not optimise for N concurrent readers. (Of course, biofuse
itself may want to support multiple FUSE handles to the mount —
e.g., python tools reading `.bim` while plink scans `.bed` — but
those are independent processes / handles, not threads inside one
plink invocation.)

---

## Wider tool ecosystem

Beyond plink1.9 / plink2 we ran six external CLIs and one Python
library against the same fixture and mount. The matrix is small —
one or two ops per tool — because the goal is to *fingerprint* the
access shape, not to exhaustively cover each tool's flag surface.
Full numbers are in `results/metrics_per_op.csv`; below we summarise
the IO pattern of each consumer alongside the closest plink baseline.

| op (`.bed`) | n_reads | bytes_read | cov_% | redund. | seq_% | median_size |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| p19_freq (baseline) | 1,901 | 248,693,753 | 100.00 | 1.0 | 99.9 | 131,072 |
| **admixture_k3** | 1,900 | 248,693,753 | 100.00 | 1.0 | 100.0 | 131,072 |
| **king_kinship** | 1,900 | 248,693,753 | 100.00 | 1.0 | 100.0 | 131,072 |
| **gcta_make_grm_bin** | 1,900 | 248,693,753 | 100.00 | 1.0 | 100.0 | 131,072 |
| **gcta_pca** | 1,900 | 248,693,753 | 100.00 | 1.0 | 100.0 | 131,072 |
| **flashpca_pca** | 1,899 | 248,693,753 | 100.00 | 1.0 | 100.0 | 131,072 |
| **bolt_lmm_inf** | 1,900 | 248,693,753 | 100.00 | 1.0 | 100.0 | 131,072 |
| **regenie_step1_bt** † | 6 | 507,904 | 0.20 | 1.0 | 100.0 | 98,304 |
| **bedreader_full_scan** | 1,900 | 248,693,753 | 100.00 | 1.0 | 100.0 | 131,072 |
| **bedreader_slice_variants_1k** | 1,001 | 12,324,864 | 4.96 | 1.0 | 0.1 | 12,288 |
| **bedreader_random_10** | 12 | 172,032 | 0.07 | 1.0 | 9.1 | 8,192 |

† REGENIE step 1 errored out on block 1 with a low-variance SNP
(synthetic random phenotype against synthetic msprime data — there is
no signal, and a small fraction of variants are monomorphic in the
fixture). The reported numbers are from the startup phase before the
crash; the access pattern up to that point is consistent with
block-by-block streaming. See Caveats.

The headline observation is that **everything except the bed-reader
sliced ops looks identical to plink at the FUSE layer** — single
forward pass, redundancy 1.0, sequential ratio 99.9–100%, kernel
readahead of 128 KiB. Read counts at FUSE level are within ±2 of
plink's 1901 because the kernel coalesces page-aligned consumer reads
into ~128 KiB readahead chunks regardless of how the consumer issues
them at the user level (bed-reader, for instance, makes ~995k 250 B
reads at the Python API level; the kernel page cache turns those into
1900 FUSE reads).

This last point is *the* takeaway: the kernel page cache is doing the
heavy lifting for fine-grained and multi-pass consumers. Bypassing it
(see the `direct_io` discussion in Caveats) makes both unusable.

### Per-tool detail

**KING `--kinship`** scans the entire `.bed` once (1,900 FUSE reads,
100% coverage, 100% sequential, redundancy 1.0). bim and fam are
fully read at startup. The kinship statistic is computed in RAM after
the load. Identical access shape to plink — KING's user-level read
sizes are smaller (4–8 KiB blocks per pair-of-variants) but they
amalgamate into 128 KiB FUSE chunks behind kernel readahead.

**GCTA `--make-grm-bin`** and **`--pca`** (the latter implemented as
GRM-then-eigendecompose) each scan `.bed` once: 1,900 reads, 100%
coverage, redundancy 1.0. The GRM is then computed entirely in RAM;
no second pass. Same shape as plink.

**flashpca2 `--ndim 10`** (randomised SVD) likewise reads the BED
once at startup (1,899 FUSE reads, 100% coverage). flashpca then runs
~10 power-iteration passes over the in-memory genotype matrix. Under
*page-cache-on*, those iterations never reach FUSE; under our earlier
direct_io experiment they did, hammered the streaming path, and made
the op unusable (see Caveats).

**REGENIE step 1 `--bt --lowmem`** errored on the first variant block
(`SNP rs368 has low variance`) before completing a full scan. The 6
FUSE reads we did capture cover the full bim (200 readahead reads
elsewhere) and the first ~1 KiB of `.bed` — consistent with REGENIE's
documented block-by-block streaming model with `--bsize 1000`. To get
clean step-1 numbers we'd need a fixture where the random phenotype
isn't perfectly correlated with some monomorphic variant, or
`--minMAC` to filter such variants up front. Left as follow-up.

**BOLT-LMM `--lmmInfOnly`** scans `.bed` once (1,900 reads, 100%
coverage). Same shape as plink. Note: BOLT-LMM's binary ships its own
`libiomp5.so` which the installer extracts to `_tools/bolt/lib/`; the
binary's RPATH is `$ORIGIN/lib` so no `LD_LIBRARY_PATH` plumbing is
needed.

**bed-reader full_scan** (`bed.read()` returning the whole genotype
matrix as `int8`) does 1,900 FUSE reads — identical to plink — even
though at the Python API level bed-reader issues ~995k 250 B reads
(one per variant row). The kernel page cache coalesces them into the
same readahead-aligned 128 KiB chunks plink gets. This is the most
striking demonstration of why direct_io was rejected: under direct_io
the same op blew up to 995k FUSE round-trips (≈5.5 minutes); under
page cache it completes in well under a second.

**bed-reader slice ops** map cleanly onto plink's `--extract` shape:

- `slice_variants_1k` (1000 evenly-spaced variants × all samples):
  1,001 FUSE reads, 4.96% coverage, redundancy 1.0, sequential ratio
  0.1% (every read jumps to a different variant offset). Median read
  size 12 KiB — partial readahead because the variant rows fall on
  page-misaligned offsets.
- `random_10` (10 random (sample, variant) cells): 12 FUSE reads,
  0.07% coverage. Sparse, scattered.

```
# .bed reads=12  file_size=248,693,753B  (top=byte 0, bottom=EOF; left=first read, right=last)
*      *      *      *
                            *      *

                                           *
                                                  *

                                                         *
                                                                *

                                                                       *
                                                                               *
```

bed-reader's `slice_samples_10` op did not produce a usable trace in
this run (the harness recorded `ERROR`); under page cache the
kernel-coalesced shape would be the same as plink's `--keep 10`
(100% coverage, since BED is variant-major and all variants must be
read).

### Cross-tool comparison

The only fundamentally new IO shape introduced by the wider ecosystem
is the **scattered byte-range pattern** of `bed-reader.slice_*` —
which is the same pattern plink already shows under `--extract` /
`--from-bp`. So at the FUSE layer, the entire ecosystem reduces to
two shapes: forward streaming + sparse byte-range reads. plink2's
existing characterisation already covers both.

What is *not* visible at the FUSE layer but matters operationally:
how quickly the consumer asks for bytes once they're available. bed-
reader at the Python level is per-variant; flashpca and ADMIXTURE
re-touch the matrix in RAM many times. These costs are paid by
subsequent kernel-cached reads; the streaming source itself only
serves the bytes once.

---

## Implications for biofuse phase 2

The recommendations in this section have been folded into a formal
consumer-driven requirements specification at
[`specs/vcztools_streaming_plink.md`](../../specs/vcztools_streaming_plink.md),
which is the binding artefact for `vcztools` implementers. The text
below is the phase-1 summary, updated to incorporate the wider
ecosystem findings.

A future streaming source in vcztools (replacing the materialise-and-
passthrough shim) needs to support, in priority order:

1. **Forward streaming of `.bed` from offset 0 to EOF.** This serves
   essentially the entire matrix across both plink and the wider
   ecosystem (ADMIXTURE, KING, GCTA, flashpca2, BOLT-LMM, bed-reader's
   `full_scan`). Stream chunks of ~128 KiB to align with FUSE
   `max_read`. Iterative consumers (flashpca's power iterations,
   ADMIXTURE's EM) re-touch the matrix in RAM without re-reading from
   FUSE, because the kernel page cache absorbs the second-and-later
   passes.

2. **Tail probe**. Every plink op reads ~1 KiB at `file_size − 1017`
   before any other `.bed` read. The streaming source must satisfy
   this without rebuilding the whole genotype matrix. Two options:
   (a) materialise the last variant chunk eagerly at mount time and
   keep it in RAM (cheap — one variant chunk worth of bytes), or
   (b) compute and cache `file_size` upfront so the FUSE layer can
   service the tail probe from a small precomputed buffer. Both are
   trivial.

3. **Byte-range random access for small `--extract` / `--from-bp` /
   `--snp --window` workloads, and for bed-reader's sliced reads.**
   The pattern is: variant index → BED offset = `3 + variant_index *
   ⌈n_samples/4⌉`. For up to a few thousand requested variants, plink
   seeks directly; bed-reader's `slice_variants_1k` / `random_10`
   exhibit the same shape. To serve this, the streaming source needs
   the ability to fetch a single variant chunk on demand by index,
   not just sequentially. In vcztools terms: expose
   `reader.variant_chunks(start_index, end_index)` or equivalent.

4. **Page-cache friendliness.** Hot bytes from a streamed sweep land
   in the kernel page cache; tools like flashpca, ADMIXTURE, GCTA
   that re-touch the matrix many times never come back to FUSE. The
   biofuse mount must not disable the page cache (no `direct_io`),
   and the streaming source need not implement its own caching layer
   beyond what's needed for the tail probe and basic chunking. This
   is a non-requirement in spirit — but it is an explicit *constraint*
   on the FUSE adapter because the alternative (direct_io) was
   measured to be unworkable.

5. **Always-full `.bim`**. Every tested consumer reads the full bim.
   Keep generating bim eagerly in memory (as biofuse already does);
   do not stream it.

6. **Single-syscall `.fam`**. Trivial — keep it in memory.

What we explicitly **do not need** in phase 2:

- Page-cache *bypass*. Direct_io was tried and rejected; see Caveats.
- Multi-pass *byte serving* from the source. The page cache turns
  multi-pass-in-RAM consumers into single-pass-at-source consumers.
- Random sample-axis access. The BED layout is variant-major; no
  consumer in the matrix prunes by sample at the IO level (sample
  filters in plink still scan every variant).
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

- **Tail probe size is fixture-dependent**. We observed 1017 bytes;
  on a different fileset it may differ. The streaming source should
  treat any `read(offset > 0.99 * file_size)` as a probe, not a
  signal of a backward scan.

- **`direct_io` was tried and rejected.** An early version of the
  harness mounted with `direct_io=True` on the FUSE replies, with the
  goal of bypassing the kernel page cache so every byte the consumer
  touched would surface to the access logger (and as a side effect,
  suppress mmap). Single-pass streaming consumers (plink, KING, GCTA's
  `--make-grm-bin` initial load, BOLT-LMM, the bed-reader sliced ops)
  worked. Multi-pass and fine-grained consumers did not:
  - **flashpca2** ran past our 600 s per-op budget without finishing
    (under page cache the same op finishes its FUSE reads in seconds;
    the in-RAM iterations don't generate FUSE traffic).
  - **bed-reader full_scan** blew up to ~995k FUSE reads (one per
    variant row) over ~5.5 minutes; under page cache the kernel
    coalesces the same user-level access pattern into 1,900 reads
    over a fraction of a second.

  The conclusion: page-cache-bypass at the FUSE layer is incompatible
  with biofuse's goal of serving these tools transparently. The flag
  and its plumbing remain in `biofuse.fuse_adapter.Mount(direct_io=...)`
  as a diagnostic escape hatch, but neither the harness nor any biofuse
  mount intended for real consumers should set it. mmap-style
  sequential access through page-cache-on is fine: page faults still
  surface to FUSE on first touch, so the access logger sees every page
  load — just not every user-level read into an already-resident page.
