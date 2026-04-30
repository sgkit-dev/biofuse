# vcztools streaming plink API — requirements specification

**Status:** draft, awaiting review by `vcztools` maintainers.
**Owner (consumer):** biofuse phase 2.
**Owner (implementer):** `vcztools`.
**Empirical basis:**
[`experiments/io-study/report.md`](../experiments/io-study/report.md)
(commit `0e472ab`).

This document specifies a new `vcztools` API,
**`vcztools.plink_streaming.PlinkStreamingSource`**, that exposes a
VCF Zarr (VCZ) store as the byte content of a plink-1 binary fileset
(`.bed` / `.bim` / `.fam`) for read-only consumption by biofuse's FUSE
mount in phase 2.

The contract is consumer-driven: biofuse identified the access patterns
empirically, and this spec captures the minimal surface that supports
them. It is binding on the API names and behaviours; it is illustrative
on argument types, constants, and internal layout — implementers MAY
substitute equivalents that achieve the documented behaviours and
performance targets.

Normative keywords (MUST / SHOULD / MAY) follow [RFC 2119](
https://www.rfc-editor.org/rfc/rfc2119).

---

## 1. Context

biofuse phase 1 mounts a plink view of a VCZ store by calling
`vcztools.plink.write_plink` to materialise the entire fileset to a
temporary directory, then exposing that directory through a passthrough
FUSE adapter. This is fast for small stores but pays a O(num_variants
× num_samples) up-front cost on every mount and consumes local disk
proportional to the genotype matrix — neither acceptable when the
target VCZ is multi-terabyte and lives on remote object storage.

Phase 2 replaces the materialisation step with a **streaming source**
in `vcztools` that biofuse calls byte-range reads against. The IO
study committed at `0e472ab` ran plink1.9 / plink2 across 43
representative operations and showed that plink's access patterns —
forward streaming, tail probe, contiguous and sparse byte-range reads
— map cleanly to a small set of variant-axis primitives `vcztools`
already has (`reader.variant_chunks`, `reader.set_variants`,
`reader.set_samples`). This spec describes the wrapper that exposes
those primitives in plink's coordinate system.

Single-process, Python-only assumption. Single FilesystemView mount
per `PlinkStreamingSource` instance; multiple in-flight FUSE handles
share one source.

---

## 2. Scope and non-goals

### In scope

- Read-only access to a VCZ store as virtual `.bed` / `.bim` / `.fam`
  bytes.
- All access patterns the IO study observed:
  - forward sequential stream of `.bed` from offset 0;
  - tail probe at `[bed_size − 4096, bed_size)`;
  - byte-range reads at arbitrary `(offset, size)`;
  - contiguous variant-range reads;
  - sparse variant-index reads (small `--extract` lists).
- Backend-storage forwarding (local dir, .zip, fsspec, obstore,
  icechunk) via the existing `vcztools.utils.open_zarr` plumbing.

### Non-goals

- **plink2 native format** (`.pgen` / `.pvar` / `.psam`). Out of scope
  until vcztools grows native pgen support.
- **Write semantics.** No mutation of the VCZ store, no buffered
  modifications, no upload paths.
- **Multi-pass `.bed` guarantees.** The IO study showed no plink op
  re-reads bytes within a run; the streaming source MAY freely drop
  bytes after they are consumed by a stream.
- **Multi-process IPC.** Single Python process per source; biofuse
  serves multiple file handles via threads.
- **Cross-VCZ joins, concatenation, reordering, multi-source
  merging.** One source wraps one VCZ store; the order of variants
  in `.bed` matches the order in the store.
- **Sample-axis random access.** The plink-1 BED layout is variant-
  major and packs all samples within each variant row, so sample
  filters cannot prune `.bed` IO. The IO study confirmed `--keep` /
  `--remove` always read 100 % of `.bed`. This API does not accept a
  per-call sample subset; if sample filtering is needed, it MUST be
  configured at construction time.
- **Backwards-compatibility shims.** The API is fresh; pre-1.0 it
  MAY change.

---

## 3. Glossary

- **VCZ** — VCF Zarr; a zarr group with the schema bio2zarr / vcztools
  produce.
- **Variant chunk** — the variant-axis chunk size of the underlying
  zarr arrays (`reader.variants_chunk_size`). Typically 10⁴ variants.
- **Sample chunk** — analogous on the sample axis.
- **BED magic** — the three-byte header `0x6C 0x1B 0x01` at offset 0
  of a plink-1 `.bed` file.
- **Bytes-per-variant (`bpv`)** — `ceil(num_samples / 4)`. Each
  variant occupies exactly `bpv` consecutive bytes after the magic.
- **`bed_size`** — `3 + num_variants * bpv`. The exact size of the
  virtual `.bed` file.
- **a12** — the per-variant pair `(allele1, allele2)` plink uses to
  emit each variant's row of `.bed`. Computed independently per
  variant chunk by `vcztools.plink.Writer._compute_alleles`. Stored
  as an `int8` array of shape `(num_variants, 2)`.
- **Tail probe** — plink's first read of `.bed` is a small
  (≈ 1 KiB) read near `bed_size`, before any forward scan. Empirical
  observation from the IO study.

---

## 4. Functional requirements

### FR-1 (Construction)

The source MUST accept a VCZ url/path plus optional `backend_storage`
and `storage_options`, forwarded unchanged to
`vcztools.utils.open_zarr` (see
`vcztools/vcztools/utils.py:109`).

### FR-2 (Eager metadata)

`num_variants`, `num_samples`, `bytes_per_variant`, `bed_size`,
`bim_size`, `fam_size` MUST be available on the instance immediately
after construction returns, without iterating genotype chunks.

### FR-3 (Eager `.fam`)

`.fam` bytes MUST be computed at construction. The IO study confirmed
plink reads `.fam` in a single syscall on every operation, so eager
materialisation is always paid back. Cost is O(num_samples · 50)
bytes — negligible.

### FR-4 (Eager-by-default `.bim`)

`.bim` bytes MUST be available on first access. The default
construction parameter `eager_a12=True` MUST cause a one-pass forward
read of all genotype chunks during `__init__` to populate the a12
cache so that `.bim` is ready synchronously thereafter.

If `eager_a12=False`, the first `bim_bytes` access MUST block the
caller until the equivalent pass completes; subsequent accesses are
O(1).

Rationale: the IO study showed every plink op reads `.bim` before
issuing forward-stream `.bed` reads. Paying the a12 cost up front
avoids surprising stalls.

### FR-5 (Forward stream)

`stream_bed()` MUST return a fresh generator on each call. Each
generator MUST yield bytes that, when concatenated, exactly match the
canonical plink-1 BED encoding (magic header followed by variant rows
in store order).

Multiple in-flight generators on the same source MUST NOT interfere
with each other or share iteration state. The source MUST be safe to
call `stream_bed()` from any thread that holds a reference to it.

Implementations MAY choose any chunk size for yielded bytes; SHOULD
default to ≥ 1 MiB (FUSE prefers ≥ 128 KiB and the per-yield Python
overhead amortises better at 1 MiB). Yielded chunk boundaries are not
observable to consumers — only the concatenation matters.

### FR-6 (Tail probe)

`read_tail(nbytes)` MUST serve any read in `[bed_size − nbytes,
bed_size)` for `nbytes ≤ 4096` without touching variants outside the
final chunk. Latency target: < 100 ms p50 on local stores; ≤ one
network round-trip on remote stores.

`nbytes > bed_size` is clamped to `bed_size`; `nbytes ≤ 0` is a
`ValueError`.

### FR-7 (Byte-range reads)

`read_bed(offset, size) -> bytes` MUST honour POSIX-`pread`-style
semantics:
- `offset < 0` raises `ValueError`.
- `offset ≥ bed_size` returns `b""`.
- `size > bed_size − offset` is silently clamped; result has length
  `bed_size − offset`.
- Never raises on EOF overrun.
- Returned bytes are a subrange of the canonical `.bed` content
  (FR-5 invariant).

The implementation MUST translate `(offset, size)` to a variant range
via the `bpv` formula:
```
v_start = max(0, offset - 3) // bpv
v_end   = ceil((min(offset + size, bed_size) - 3) / bpv)
```
and dispatch through `read_variants(slice(v_start, v_end))`, slicing
the result to the exact byte window.

### FR-8 (Variant-range reads)

`read_variants(indexes, *, strategy="auto") -> bytes` MUST accept
either a `slice` (contiguous range) or a sorted `np.ndarray[int]` of
variant indexes. Returns the encoded BED bytes for those variants
concatenated in input order. **Magic header is never emitted by
`read_variants`** — it is exclusively a `read_bed` / `stream_bed`
concern.

`strategy`:
- `"contiguous"` — input MUST be a slice; uses
  `reader.set_variants([ChunkRead(...)])` covering the range.
- `"sparse"` — input MUST be a sorted ndarray; uses
  `reader.set_variants(indexes_array)` and exploits per-chunk local
  selections.
- `"auto"` (default) — picks `"sparse"` when
  `len(indexes) / num_variants < SPARSE_VARIANT_THRESHOLD` (1 %)
  AND the indexes span more than `SPARSE_VARIANT_THRESHOLD ·
  num_variants` of the variant axis (densely-packed small ranges
  still go contiguous because the chunk fetch cost is identical).
  Otherwise `"contiguous"` over `[min(indexes), max(indexes) + 1)`.

Edge cases:
- Empty selection → `b""`.
- Unsorted ndarray → `ValueError`.
- Out-of-range indexes (< 0 or ≥ num_variants) → `IndexError`.
- Duplicates in ndarray → reflected in output (caller responsibility).

### FR-9 (Lifecycle)

`close()` MUST release the underlying zarr resources, drop the a12
cache, and mark the source closed. `__enter__` and `__exit__` MUST
work; `__exit__` calls `close`. `close()` is idempotent.

After `close()`, subsequent calls to `stream_bed`, `read_bed`,
`read_variants`, `read_tail`, or any property that requires the
underlying store MUST raise `RuntimeError("source closed")`. Active
generators returned by `stream_bed` are NOT cancelled synchronously
(forcibly killing readahead worker threads is not exposed by
`vcztools.retrieval`); they MAY drain their already-prefetched
window and then raise on the next `__next__`.

### FR-10 (Errors)

Upstream zarr / store errors propagate at the failing call site:
- During `stream_bed` iteration: raised on the `__next__` that touched
  the failing chunk.
- For `read_bed` / `read_variants` / `read_tail`: raised from the
  call.

`read_bed` is EOF-tolerant per FR-7; it MUST NOT raise on overrun.
`read_variants` raises `IndexError` on out-of-range variant indexes.
Construction errors (missing fields, non-biallelic store, network
failure) propagate from `__init__`.

---

## 5. Non-functional requirements

| Id | Requirement | Target |
| --- | --- | --- |
| NFR-1 | Forward streaming throughput | ≥ 80 % of underlying `variant_chunks` throughput on the same store. Anchor: 1200 MiB/s on local/icechunk per `vcztools/performance/benchmarks.md` ⇒ ≥ 960 MiB/s. |
| NFR-2 | Tail-probe latency | < 100 ms p50 on local stores; ≤ one round-trip on remote stores. |
| NFR-3 | Single-chunk-aligned `read_bed` | < 200 ms p50 on local stores. |
| NFR-4 | Sparse `read_variants` (≤ 10 variants spanning ≤ 10 chunks) | < 1 s p50 on local stores. |
| NFR-5 | Memory: a12 cache | `2 · num_variants` bytes (int8 cache). |
| NFR-5 | Memory: working set | Bounded by the underlying `variant_chunks` readahead window (default 256 MiB; configurable via `readahead_bytes`). |
| NFR-6 | Concurrency | All public methods MUST be safe to call from multiple threads. Mutable state (a12 cache, fill mask) protected by a single lock; the read-side fast path MUST be lock-free after first publication. |
| NFR-7 | Determinism | Each call to `stream_bed()`, `read_bed(offset, size)`, `read_variants(idx)`, `read_tail(n)` returns byte-identical output for byte-identical inputs against the same store. |
| NFR-8 | Read-only | The source MUST NOT modify the underlying VCZ store. |

Measurement approach: re-run
[`experiments/io-study/run.py`](../experiments/io-study/run.py) against
biofuse mounted on top of the streaming source instead of the
materialised passthrough. The resulting `metrics.csv` is directly
comparable to the phase-1 baseline. NFR-1 / NFR-3 / NFR-4 fall out of
the elapsed-time and bytes-read columns; NFR-2 from a small targeted
benchmark.

---

## 6. API contract

The following surface is binding on names and observable behaviour.
Argument types, constants, and types of intermediate values are
illustrative — implementations MAY substitute equivalents that
achieve the documented behaviours and performance targets.

### Signature

```python
class PlinkStreamingSource:
    """Read-only streaming view of a VCZ store as plink-1 BED/BIM/FAM."""

    BED_MAGIC: ClassVar[bytes] = b"\x6c\x1b\x01"
    DEFAULT_STREAM_CHUNK: ClassVar[int] = 1 << 20      # 1 MiB
    SPARSE_VARIANT_THRESHOLD: ClassVar[float] = 0.01    # 1 %

    def __init__(
        self,
        path: str | Path,
        *,
        backend_storage: str | None = None,
        storage_options: dict | None = None,
        readahead_workers: int | None = None,
        readahead_bytes: int | None = None,
        eager_a12: bool = True,
    ) -> None: ...

    # ---- Eager metadata ----------------------------------------------------
    @property
    def num_variants(self) -> int: ...
    @property
    def num_samples(self) -> int: ...
    @property
    def bytes_per_variant(self) -> int: ...        # ceil(num_samples / 4)
    @property
    def bed_size(self) -> int: ...                 # 3 + num_variants * bpv
    @property
    def bim_size(self) -> int: ...
    @property
    def fam_size(self) -> int: ...

    # ---- Eager small resources --------------------------------------------
    @property
    def bim_bytes(self) -> bytes: ...
    @property
    def fam_bytes(self) -> bytes: ...

    # ---- Tail probe -------------------------------------------------------
    def read_tail(self, nbytes: int = 4096) -> bytes: ...

    # ---- Forward streaming ------------------------------------------------
    def stream_bed(self, *, chunk_size: int | None = None) -> Iterator[bytes]: ...

    # ---- Random access ----------------------------------------------------
    def read_bed(self, offset: int, size: int) -> bytes: ...
    def read_variants(
        self,
        indexes: slice | np.ndarray,
        *,
        strategy: str = "auto",
    ) -> bytes: ...

    # ---- Lifecycle --------------------------------------------------------
    def close(self) -> None: ...
    def __enter__(self) -> "PlinkStreamingSource": ...
    def __exit__(self, *exc) -> None: ...
```

### Behavioural clauses

**`__init__`** — Forwards `path`, `backend_storage`, `storage_options`
to `vcztools.utils.open_zarr`, then constructs a private `VczReader`
(`vcztools/vcztools/retrieval.py:468`) with the supplied readahead
knobs. Reads static fields needed for `.bim` / `.fam` (sample IDs,
contigs, positions, alleles, optional `variant_id`) and validates the
store is biallelic (re-using `Writer._compute_alleles`'s constraint
at `vcztools/vcztools/plink.py:107` and matching its `ValueError`).
Builds `fam_bytes` immediately. If `eager_a12=True`, runs the full
forward genotype pass to populate the a12 cache; otherwise defers.
Edge cases: missing fields → propagates `KeyError` with a wrapping
message; non-biallelic → `ValueError`; empty store
(`num_variants == 0`) is valid and yields a 3-byte `.bed`.

**Metadata properties** — All O(1) after `__init__`. `bed_size` is
derived from the BED layout formula, never from a separate IO path.
`bim_size` equals `len(self.bim_bytes)` (forces a12 materialisation
on first access if `eager_a12=False`); `fam_size` equals
`len(self.fam_bytes)`.

**`bim_bytes` / `fam_bytes`** — Returned as `bytes` (UTF-8 from the
existing `vcztools.plink.generate_bim` / `generate_fam` strings).
Cached for the source lifetime. `.bim` access requires a populated
a12 cache; first access blocks if `eager_a12=False`.

**`read_tail(nbytes=4096)`** — Returns the last `min(nbytes,
bed_size)` bytes. Internally computes the variant range covering
`[bed_size − nbytes, bed_size)` and runs a single short
variant-chunk read with `set_variants`. Operates at chunk
granularity for a12 (computes a12 for the entire last variant chunk
and populates the cache) so the cache invariant
"chunk-marked-filled means the chunk's a12 is fully present" is
preserved. `nbytes ≤ 0` → `ValueError`. `nbytes > bed_size` is
clamped silently.

**`stream_bed(chunk_size=None)`** — Returns a fresh generator each
call. Internally drives `reader.variant_chunks(fields=[
"call_genotype", "variant_allele"])` over a scoped `VczReader`
clone so each generator owns its own iteration state. For each
chunk, computes a12 (re-using `_compute_alleles`), encodes via
`vcztools.plink.encode_genotypes`, write-throughs into the a12
cache, and re-buffers bytes into `chunk_size`-sized fragments
(default `DEFAULT_STREAM_CHUNK` = 1 MiB). The first yielded fragment
is prefixed with `BED_MAGIC`. Stopping early (generator close) is
safe; the underlying readahead pipeline drains on garbage
collection.

**`read_bed(offset, size)`** — The FUSE-friendly entrypoint.
Per FR-7 semantics. Translates byte ranges to variant ranges via
the `bpv` formula and delegates to
`read_variants(slice(v_start, v_end), strategy="contiguous")`,
slicing the result to the exact byte window. Offsets in `[0, 3)`
slice into `BED_MAGIC` directly without touching variant data.

**`read_variants(indexes, strategy="auto")`** — Per FR-8 semantics.
Always uses an a12 slice consistent with the source-wide cache: if
the relevant chunks are cached, slices the cache; otherwise computes
a12 on the fly for the chunks visited and writes-through into the
cache. Magic header is never emitted.

**`close` / `__enter__` / `__exit__`** — Per FR-9 semantics.
Releases the zarr store (the source always owns it; `open_zarr` is
called from `__init__`), drops the a12 cache, marks the source
closed. Idempotent.

### What is binding vs illustrative

**Binding** (biofuse will write integration code against these names
today):

`PlinkStreamingSource`, `stream_bed`, `read_bed`, `read_variants`,
`read_tail`, `bim_bytes`, `fam_bytes`, `num_variants`, `num_samples`,
`bytes_per_variant`, `bed_size`, `bim_size`, `fam_size`,
`BED_MAGIC`, `close`, `__enter__`, `__exit__`. The constructor MUST
accept `path` as its first positional argument and the listed keyword
arguments by name.

**Illustrative** (implementer MAY tune):

The defaults `DEFAULT_STREAM_CHUNK = 1 << 20` and
`SPARSE_VARIANT_THRESHOLD = 0.01` — these MUST exist with values
that meet NFR-1..NFR-4, but their numeric magnitudes may change.
The `strategy` parameter on `read_variants` MAY have additional
values beyond `auto`/`contiguous`/`sparse`.
The internal data structures in §7 are non-binding guidance.

---

## 7. Internal design notes (non-binding)

These notes describe one viable implementation. Implementers MAY
substitute equivalents that satisfy the public contract.

### a12 cache

- Shape `(num_variants, 2)`, dtype `int8` — narrower than `Writer`'s
  `int64` (which the existing code uses internally) because
  `encode_genotypes` casts to `G.dtype = int8` anyway. Memory cost
  `2 · num_variants` bytes ≈ 2 MB for a 1 M-variant store.
- Companion `_a12_filled: np.ndarray[bool]` of length
  `num_chunks` records which chunks are populated. Read-side checks
  the mask before trusting a slice.
- `bim_bytes` access requires every chunk filled; if not, runs a
  one-shot forward pass over the gaps. This is typically triggered
  exactly once early in the mount lifetime under `eager_a12=True`,
  then becomes permanent.

### Lock policy

A single `threading.Lock` guards writes to `_a12_cache` /
`_a12_filled`. The read fast path is a relaxed mask check followed
by an immutable-array slice — no lock acquisition once the relevant
chunk is published. Cached `bim_bytes` / `fam_bytes` are written
once during `__init__` (fam) or first access (bim under the same
lock) and treated as immutable thereafter.

### `VczReader` cloning

A long-lived `VczReader` is held for static metadata access (sample
IDs, contigs, positions, alleles, `variant_id`). Iterations and
ranged reads use scoped clones — either fresh
`VczReader(root=self._reader.root)` instances per call (cheap if
`__init__` doesn't re-read static fields, which the cached-property
pattern in `retrieval.py` suggests it doesn't), or a clone helper
once `VczReader` exposes one. The decision matters for performance,
not correctness.

### Chunk reuse

When `read_bed` covers a range whose a12 is already fully cached,
only `call_genotype` is read from zarr; `_compute_alleles` is
skipped. This is the steady-state path under FUSE once a consumer
has done one full scan (e.g. after `--make-bed` or `--pca`).

---

## 8. Performance targets

(Replicates NFR-1..NFR-5 here for easy reference. Same numbers, no
new requirements.)

| Metric | Target |
| --- | --- |
| Forward stream throughput (local/icechunk) | ≥ 960 MiB/s |
| Tail-probe latency (p50, local) | < 100 ms |
| Tail-probe latency (remote) | ≤ 1 round-trip |
| `read_bed` single-chunk-aligned (p50, local) | < 200 ms |
| `read_variants` sparse, ≤ 10 variants (p50, local) | < 1 s |
| a12 cache memory | `2 · num_variants` bytes |
| Working memory beyond a12 | bounded by `readahead_bytes` (default 256 MiB) |

**How to measure**: re-run
[`experiments/io-study/run.py`](../experiments/io-study/run.py) against
a biofuse mount whose `plink_source` has been swapped from the
materialised passthrough to a streaming-source-backed adapter. The
existing `analyze.py` produces a directly-comparable metrics CSV.
Sub-100 ms tail-probe and read-bed targets need a small targeted
benchmark — recommend adding it to `vcztools/performance/`.

---

## 9. Acceptance criteria

The streaming source is "done" when:

1. **biofuse can swap implementations cleanly.** The body of
   [`biofuse/biofuse/plink_source.py`](../biofuse/plink_source.py)'s
   `open()` can be replaced with a `PlinkStreamingSource(...)` call
   plus a thin adapter that exposes it as the existing
   `FilesystemView` interface from
   [`biofuse/biofuse/view.py`](../biofuse/view.py). The adapter's
   `read(fh, offset, size)` calls `read_bed(offset, size)`; `list()`
   / `stat()` are derived from the metadata properties.

2. **biofuse's plink-app tests pass unchanged.**
   [`tests/test_plink_apps.py`](../tests/test_plink_apps.py) runs
   plink1.9 / plink2 against the mounted source and must produce
   byte-identical outputs to the phase-1 baseline. This proves end-
   to-end semantic preservation across all 43 IO-study operations.

3. **A new `test_streaming_source.py`** (location: vcztools or
   biofuse, implementer's choice) covers, at minimum:
   - tail probe at multiple `nbytes` values;
   - `read_bed` at offsets `0`, `1`, `2`, `3`, `bed_size − 1`,
     `bed_size`, `bed_size + 100`;
   - `read_bed` with `size` of `0`, `1`, `bpv`, `bed_size`, and
     overruns;
   - contiguous and sparse `read_variants`;
   - empty selection (`slice(0, 0)`, `np.array([])`);
   - out-of-range indexes (raises `IndexError`);
   - unsorted ndarray (raises `ValueError`);
   - byte-identical match between `b"".join(stream_bed())` and
     concatenated `read_bed` over the whole file;
   - concurrent streams from multiple threads producing identical
     output;
   - `close()` then call → `RuntimeError`;
   - error propagation from a deliberately broken zarr store.

4. **The IO-study report reproduces.** Re-running
   [`experiments/io-study/run.py`](../experiments/io-study/run.py)
   against the streaming source yields a `metrics.csv` whose
   per-(op, file) `coverage_pct`, `redundancy`, `sequential_pct`
   columns match the phase-1 baseline within rounding. Bytes-served
   per op are identical because plink asks for the same byte
   ranges.

5. **Performance targets in §8 are met** on a modern laptop / CI
   runner against the IO-study fixture (~250 MiB BED) and at least
   one larger fixture (~10 GiB BED if feasible) on local zarr.

---

## 10. Out of scope

Listed in §2 as non-goals; recapped here with brief rationale so
contributors can short-circuit the same questions:

- **plink2 .pgen** — `vcztools` has no pgen writer yet. Spec a
  separate API once that lands.
- **Write semantics** — biofuse is read-only by design.
- **Multi-pass `.bed` guarantees** — empirically not needed.
- **Multi-process IPC / shared-memory `.bed`** — single-process
  Python is sufficient for the FUSE consumer.
- **Cross-VCZ joins / concat / reorder** — one source per VCZ.
- **Sample-axis random access** — BED layout precludes it; `--keep`
  / `--remove` always do a full scan per the IO study.
- **Backwards-compatibility shims** — pre-1.0 surface; biofuse will
  pin a vcztools version.

---

## 11. Open questions

These are flagged for the implementer to resolve during build and
record in the spec follow-up:

1. **Cheap `VczReader` cloning.** Does `VczReader.__init__` re-read
   static fields, or are those cached? If re-read, a clone helper
   is needed; if cached (likely from inspection), per-call fresh
   construction is fine.
2. **a12 cache dtype.** The proposed `int8` saves memory and matches
   `encode_genotypes`'s expected dtype, but `Writer._compute_alleles`
   currently returns `int64`. Verify int8 is sufficient for biallelic
   stores (it is — values are 0 or 1 — but worth confirming under
   the existing test suite).
3. **`eager_a12=True` default.** The IO study suggests every plink
   op reads `.bim` before `.bed` streaming, justifying eager. If
   profiling shows `eager_a12=False` is acceptable for the streaming
   path (`.bim` blocks only when actually accessed), revisit.
4. **Tail-probe a12 chunk granularity.** The proposed approach is
   "compute a12 for the entire last variant chunk and populate the
   global cache" so the cache invariant stays clean. Confirm this is
   acceptable; it slightly over-reads on remote stores but keeps the
   data structure simple.

The following questions were resolved by this spec:

5. ~~`stream_bed(start=...)` — needed?~~ **Dropped.** The IO study
   showed every consumer starts at offset 0; compose `read_bed` +
   `stream_bed()` if a partial forward stream is ever needed.
6. ~~Magic header in `read_variants`?~~ **Never.** Magic is solely a
   `read_bed` / `stream_bed` concern.
