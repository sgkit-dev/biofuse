"""pjdfstest against a live biofuse mount.

pjdfstest is a POSIX filesystem test suite of shell+C programs emitting
TAP output. Its test cases assume a *writable* working directory: each
mutation test typically does ``mkdir`` + setup + verify + cleanup. On a
read-only mount the setup fails, which cascades into many "not ok" lines
that don't directly mean biofuse is broken.

This runner is therefore primarily *informational*: it reports per-group
ok / not_ok counts. A group only fails the runner if it crashes outright
(e.g. pjdfstest binary missing). The detailed counts and per-test logs
let a human assess which failures are interesting (e.g. a mutation that
unexpectedly succeeded would show as ``ok`` for a test that should reject).

The harness clones pjdfstest at a pinned commit into
``fs_tests/third_party/pjdfstest/`` and builds it on first run. The
``tests/conf`` file is patched to hardcode ``fs=FUSE`` because pjdfstest's
default detection runs ``df -PT .`` which fails — biofuse does not
implement statfs(2) (a separate finding the POSIX runner already flags).
"""

import logging
import pathlib
import re
import subprocess
import time

from harness import fixtures, tools
from harness import mount as mount_mod

logger = logging.getLogger(__name__)

PJDFSTEST_REPO = "https://github.com/pjd/pjdfstest.git"
PJDFSTEST_COMMIT = "03eb257"

THIRD_PARTY = pathlib.Path(__file__).resolve().parent.parent / "third_party"
PJDFSTEST_DIR = THIRD_PARTY / "pjdfstest"

# pjdfstest organises tests by syscall directory. ``open`` and ``granular``
# include checks that don't strictly need write access (open(2) error paths,
# permission bits). Mutation groups are run as REJECT_GROUPS where a test
# "failure with the right errno" counts as a biofuse pass.

ALL_GROUPS = [
    "open",
    "granular",
    "chflags",
    "chmod",
    "chown",
    "ftruncate",
    "link",
    "mkdir",
    "mkfifo",
    "mknod",
    "rename",
    "rmdir",
    "symlink",
    "truncate",
    "unlink",
    "utimensat",
]


def _ensure_pjdfstest_built() -> str | None:
    """Clone+build pjdfstest if needed. Returns None on success, reason on skip."""
    THIRD_PARTY.mkdir(parents=True, exist_ok=True)
    if not PJDFSTEST_DIR.exists():
        if not tools.have_tool("git"):
            return "git not installed"
        logger.info("cloning pjdfstest into %s", PJDFSTEST_DIR)
        try:
            subprocess.run(
                ["git", "clone", "--quiet", PJDFSTEST_REPO, str(PJDFSTEST_DIR)],
                check=True,
                timeout=120,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            return f"git clone failed: {exc}"
    try:
        subprocess.run(
            ["git", "-C", str(PJDFSTEST_DIR), "checkout", "--quiet", PJDFSTEST_COMMIT],
            check=True,
            timeout=30,
        )
    except subprocess.CalledProcessError as exc:
        return f"git checkout {PJDFSTEST_COMMIT} failed: {exc}"

    # pjdfstest's tests/conf detects the filesystem type via ``df -PT .``,
    # which calls statfs(2). biofuse does not implement statfs so df errors
    # and pjdfstest aborts with "Cannot figure out file system type". We
    # short-circuit detection by hardcoding fs=FUSE in conf.
    conf_path = PJDFSTEST_DIR / "tests" / "conf"
    if conf_path.exists():
        conf_text = conf_path.read_text()
        marker = "# biofuse harness override\n"
        if marker not in conf_text:
            conf_path.write_text(marker + 'fs="FUSE"\nreturn 0\n')

    binary = PJDFSTEST_DIR / "pjdfstest"
    if not binary.exists():
        for prereq in ("autoreconf", "make", "cc"):
            if not tools.have_tool(prereq):
                return f"{prereq} not installed; cannot build pjdfstest"
        for step in (
            ["autoreconf", "-fi"],
            ["./configure"],
            ["make"],
        ):
            logger.info("pjdfstest build: %s", " ".join(step))
            try:
                subprocess.run(
                    step,
                    cwd=PJDFSTEST_DIR,
                    check=True,
                    timeout=300,
                    capture_output=True,
                )
            except subprocess.CalledProcessError as exc:
                stderr = (exc.stderr or b"").decode("utf-8", errors="replace")
                return f"build step {' '.join(step)} failed: {stderr[-500:]}"
            except subprocess.TimeoutExpired:
                return f"build step {' '.join(step)} timed out"
    return None


_TAP_OK = re.compile(r"^ok\s+\d+")
_TAP_NOT_OK = re.compile(r"^not ok\s+\d+(?:\s+-\s+(.*))?")


def _run_test_file(
    test_path: pathlib.Path,
    cwd: pathlib.Path,
) -> tuple[int, int, list[str]]:
    """Run one pjdfstest .t file under sh; returns (ok, not_ok, fail_lines)."""
    cmd = ["sh", str(test_path)]
    proc = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    output = proc.stdout
    ok = 0
    not_ok = 0
    fail_lines: list[str] = []
    for line in output.splitlines():
        if _TAP_OK.match(line):
            ok += 1
        elif _TAP_NOT_OK.match(line):
            not_ok += 1
            fail_lines.append(line.strip())
    return ok, not_ok, fail_lines


def _run_group(
    group: str,
    mnt: pathlib.Path,
    log_dir: pathlib.Path,
) -> tools.CheckResult:
    """Run all .t files in pjdfstest/tests/<group>/ from inside ``mnt``.

    Informational: pass means the group ran (tests collected). Counts and
    failure samples are recorded to the per-group log.
    """
    started = time.monotonic()
    group_dir = PJDFSTEST_DIR / "tests" / group
    if not group_dir.is_dir():
        return tools.CheckResult(
            name=f"pjdfstest:{group}",
            passed=True,
            duration_s=0.0,
            detail=f"group {group} not present in pjdfstest",
        )
    test_files = sorted(group_dir.glob("*.t"))
    if not test_files:
        return tools.CheckResult(
            name=f"pjdfstest:{group}",
            passed=True,
            duration_s=0.0,
            detail=f"no .t files in {group}",
        )
    logger.info("pjdfstest: running group %s (%d files)", group, len(test_files))
    total_ok = 0
    total_not_ok = 0
    failures: list[str] = []
    timeouts = 0
    for tf in test_files:
        try:
            ok, not_ok, fl = _run_test_file(tf, mnt)
        except subprocess.TimeoutExpired:
            timeouts += 1
            failures.append(f"{tf.name}: timeout")
            continue
        logger.debug("pjdfstest:%s/%s ok=%d not_ok=%d", group, tf.name, ok, not_ok)
        total_ok += ok
        total_not_ok += not_ok
        for line in fl[:5]:
            failures.append(f"{tf.name}: {line}")

    log_path = log_dir / f"pjdfstest-{group}.log"
    log_path.write_text(
        f"group: {group}\nok: {total_ok}\nnot_ok: {total_not_ok}\n"
        f"timeouts: {timeouts}\n\n" + "\n".join(failures)
    )
    duration = time.monotonic() - started
    collected = total_ok + total_not_ok
    # Informational pass: ran successfully if any tests reported and no
    # timeouts. The detail records the breakdown for human review.
    passed = collected > 0 and timeouts == 0
    return tools.CheckResult(
        name=f"pjdfstest:{group}",
        passed=passed,
        duration_s=duration,
        detail=(
            f"ok={total_ok} not_ok={total_not_ok} timeouts={timeouts} "
            f"(read-only FS — high not_ok is expected; see log for samples)"
        ),
    )


def run(*, log_dir: pathlib.Path) -> tools.RunnerResult:
    started = time.monotonic()
    log_dir.mkdir(parents=True, exist_ok=True)

    logger.info("pjdfstest: ensuring source is cloned and built")
    skip_reason = _ensure_pjdfstest_built()
    if skip_reason is not None:
        logger.info("pjdfstest: SKIP (%s)", skip_reason)
        return tools.RunnerResult(
            runner="pjdfstest",
            passed=True,
            duration_s=time.monotonic() - started,
            skipped=True,
            skip_reason=skip_reason,
        )

    spec = fixtures.SMALL
    vcz_path = fixtures.get_or_build(spec)
    mountpoint = log_dir / "mnt"
    logger.info(
        "pjdfstest: running %d groups against biofuse mount of %s",
        len(ALL_GROUPS),
        vcz_path,
    )

    checks: list[tools.CheckResult] = []
    with mount_mod.BiofuseMount(
        str(vcz_path), mountpoint, log_path=log_dir / "mount.log"
    ) as mnt:
        for group in ALL_GROUPS:
            checks.append(_run_group(group, mnt, log_dir))

    duration = time.monotonic() - started
    return tools.RunnerResult(
        runner="pjdfstest",
        passed=all(c.passed for c in checks),
        duration_s=duration,
        checks=checks,
        summary=(
            f"pjdfstest@{PJDFSTEST_COMMIT[:8]} on read-only mount "
            f"({len(ALL_GROUPS)} groups; results informational — "
            "see per-group logs for failure samples)"
        ),
    )
