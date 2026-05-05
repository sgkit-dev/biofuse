# biofuse filesystem test harness

A standalone POSIX compliance and read-stress harness for biofuse FUSE
mounts. Lives outside the pytest suite — invoked manually, not by CI on
every PR.

## What it covers

| Category | Runner | Approx runtime |
|---|---|---|
| POSIX syscall semantics | native Python | ~10 s |
| pjdfstest curated subset | external (`pjdfstest`) | ~30–60 s |
| Read-pattern stress | external (`fio`) | ~3 min |
| Read cross-validation | native Python (fsx-style) | ~30 s |
| Filesystem stressor | external (`stress-ng`) | ~2 min |
| Mount/unmount cycling | native Python | ~2 min |
| **Total** | | **~10–15 min** |

Every category mounts biofuse fresh in a subprocess (matching real-user
behaviour) and tears it down on exit.

## Dependencies

System packages (Ubuntu 22.04+ or 24.04):

```bash
sudo apt-get install -y \
    fuse3 libfuse3-dev pkg-config \
    fio stress-ng \
    autoconf automake libtool gcc make
```

Python deps installed via uv:

```bash
uv sync --group fs-tests
```

`pjdfstest` is fetched and built on first run into
`fs_tests/third_party/pjdfstest/` (gitignored).

## Running

Run from the repository root:

```bash
uv run --group fs-tests python -m fs_tests.harness                  # all categories
uv run --group fs-tests python -m fs_tests.harness --quick          # smoke run (skips pjdfstest + large fio)
uv run --group fs-tests python -m fs_tests.harness posix            # one category
uv run --group fs-tests python -m fs_tests.harness fio --large      # fio with the 500 MB fixture
uv run --group fs-tests python -m fs_tests.harness --results /tmp/biofuse-results all
```

Output lands in `fs_tests/results/<UTC-timestamp>/`:

- `report.json` — structured per-check pass/fail/duration.
- `report.md` — human-readable summary.
- `<category>.log` — raw stdout/stderr from each runner.

Exit codes:

- 0 — every check passed.
- 1 — at least one check failed.
- 2 — harness error (missing tool, mount failed, etc.).

## Caveats

**pjdfstest is informational.** pjdfstest's tests assume a writable
filesystem (each test does `mkdir`, sets up state, verifies, cleans
up). On a read-only mount the setup fails and most tests report `not
ok` in cascading ways. The runner reports per-group ok/not_ok counts
and passes if tests ran (no timeouts / build crash); it does *not*
gate on absolute counts. Read the per-group logs in `pjdfstest-*.log`
to spot interesting failures (e.g. a `mkdir` test reporting `ok` when
it should reject would be biofuse permitting a write).

The harness also patches `pjdfstest/tests/conf` to hardcode `fs=FUSE`,
short-circuiting pjdfstest's `df -PT .` filesystem auto-detect (which
isn't always reliable on FUSE mounts and is unnecessary for our
purpose).

**stress-ng background load.** Most stress-ng filesystem stressors
need a writable working dir, so they cannot run against the mount
itself. Instead we use stress-ng (when installed) as a CPU+memory
background load and exercise the mount with an in-harness
multi-process open/read loop. The runner passes if the open-loop
completes with zero errors.

**fio throughput is informational.** The fio pass criterion is "zero
errors" reported by fio. Throughput numbers are recorded in `report.md`
for tracking but do not gate on a floor — they depend heavily on the
host (page cache, CPU, fixture size).

**fsx is read-only mode only.** Apple/LTP/xfstests fsx all assume a
writable filesystem (they bootstrap the in-memory model by writing to
the file under test). None of them run unmodified against a read-only
mount. Rather than vendor and heavily patch one, the harness
reimplements the read-only core in `harness/fsx_runner.py`: random
pread + mmap read with cross-validation against an oracle copy of the
file kept on the host filesystem.

## Adding a new check

- POSIX native: append to `harness/posix.py`. Each check is a
  `@check(name=...)` -decorated function returning `None` on pass or
  raising on fail.
- New external tool: add a runner in `harness/<tool>_runner.py`
  following the `RunnerResult` shape used by the others; register it in
  `harness/cli.py`.
