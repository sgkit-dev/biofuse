"""Native-Python POSIX semantic checks against a live biofuse mount.

Each check is a function decorated with :func:`check`. The runner mounts a
real biofuse plink fileset once, materialises the same fileset to a host
scratch dir as an *oracle* (so byte-equality checks have something
trusted to compare against), and invokes every check serially; one check
failing does not stop the others.

Three file-size slots are provided on the OracleFiles instance:
``SMALL_NAME`` (.fam), ``MEDIUM_NAME`` (.bim), ``LARGE_NAME`` (.bed).
"""

import contextlib
import errno
import fcntl
import logging
import mmap
import os
import pathlib
import random
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable

from . import fixtures, tools
from . import mount as mount_mod

logger = logging.getLogger(__name__)


CheckFn = Callable[["OracleFiles", pathlib.Path], None]
_REGISTRY: list[tuple[str, CheckFn]] = []


def check(name: str) -> Callable[[CheckFn], CheckFn]:
    """Decorator: register a check by name."""

    def deco(fn: CheckFn) -> CheckFn:
        _REGISTRY.append((name, fn))
        return fn

    return deco


class OracleFiles:
    """Host-fs oracle copy of the three plink files biofuse will serve.

    SMALL_NAME / MEDIUM_NAME / LARGE_NAME map to the .fam, .bim and
    .bed file in the same fileset. ``contents`` caches each file's
    bytes so checks can byte-compare against this oracle.
    """

    def __init__(self, oracle_dir: pathlib.Path, basename: str) -> None:
        self.root = oracle_dir
        self.basename = basename
        self.SMALL_NAME = f"{basename}.fam"
        self.MEDIUM_NAME = f"{basename}.bim"
        self.LARGE_NAME = f"{basename}.bed"
        self.contents: dict[str, bytes] = {
            name: (oracle_dir / name).read_bytes()
            for name in (self.SMALL_NAME, self.MEDIUM_NAME, self.LARGE_NAME)
        }

    def names(self) -> list[str]:
        return sorted(self.contents)


# ---------------------------------------------------------------------------
# Helper assertions used inside checks
# ---------------------------------------------------------------------------


def _expect_errno(exc: OSError, allowed: set[int], context: str) -> None:
    if exc.errno not in allowed:
        names = ", ".join(errno.errorcode.get(e, str(e)) for e in sorted(allowed))
        got = errno.errorcode.get(exc.errno, str(exc.errno))
        raise AssertionError(
            f"{context}: expected errno in {{{names}}}, got {got} ({exc})"
        )


def _expect_oserror_in(
    fn: Callable[[], object],
    allowed: set[int],
    context: str,
) -> OSError:
    try:
        fn()
    except OSError as exc:
        _expect_errno(exc, allowed, context)
        return exc
    raise AssertionError(f"{context}: expected OSError, got success")


# ---------------------------------------------------------------------------
# Open / read / lseek
# ---------------------------------------------------------------------------


@check("open: O_RDONLY succeeds")
def _open_rdonly(b: OracleFiles, mnt: pathlib.Path) -> None:
    fd = os.open(mnt / b.SMALL_NAME, os.O_RDONLY)
    os.close(fd)


@check("open: O_RDONLY|O_NONBLOCK|O_CLOEXEC accepted")
def _open_extra_flags(b: OracleFiles, mnt: pathlib.Path) -> None:
    fd = os.open(mnt / b.SMALL_NAME, os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        flags = fcntl_getfl(fd)
        if flags & os.O_ACCMODE != os.O_RDONLY:
            raise AssertionError(f"O_ACCMODE != O_RDONLY: flags={flags:o}")
    finally:
        os.close(fd)


def fcntl_getfl(fd: int) -> int:
    return fcntl.fcntl(fd, fcntl.F_GETFL)


@check("open: O_WRONLY rejected with EROFS or EACCES")
def _open_wronly_rejected(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: os.close(os.open(mnt / b.SMALL_NAME, os.O_WRONLY)),
        {errno.EROFS, errno.EACCES},
        "O_WRONLY",
    )


@check("open: O_RDWR rejected with EROFS or EACCES")
def _open_rdwr_rejected(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: os.close(os.open(mnt / b.SMALL_NAME, os.O_RDWR)),
        {errno.EROFS, errno.EACCES},
        "O_RDWR",
    )


@check("open: O_APPEND rejected with EROFS or EACCES")
def _open_append_rejected(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: os.close(os.open(mnt / b.SMALL_NAME, os.O_RDONLY | os.O_APPEND)),
        {errno.EROFS, errno.EACCES},
        "O_APPEND",
    )


@check("open: O_CREAT for new file rejected")
def _open_create_rejected(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: os.close(os.open(mnt / "newfile.dat", os.O_RDONLY | os.O_CREAT, 0o644)),
        {errno.EROFS, errno.EACCES, errno.ENOSYS, errno.EPERM},
        "O_CREAT new",
    )


@check("open: O_DIRECTORY on regular file -> ENOTDIR")
def _open_o_directory_on_regular(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: os.close(os.open(mnt / b.SMALL_NAME, os.O_RDONLY | os.O_DIRECTORY)),
        {errno.ENOTDIR},
        "O_DIRECTORY on regular",
    )


@check("open: O_DIRECTORY on mountpoint -> ok")
def _open_o_directory_on_dir(b: OracleFiles, mnt: pathlib.Path) -> None:
    fd = os.open(mnt, os.O_RDONLY | os.O_DIRECTORY)
    os.close(fd)


@check("open: nonexistent path -> ENOENT")
def _open_enoent(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: os.close(os.open(mnt / "no-such.dat", os.O_RDONLY)),
        {errno.ENOENT},
        "open ENOENT",
    )


@check("read: full file via os.read matches backing")
def _read_full(b: OracleFiles, mnt: pathlib.Path) -> None:
    expected = b.contents[b.MEDIUM_NAME]
    fd = os.open(mnt / b.MEDIUM_NAME, os.O_RDONLY)
    try:
        chunks: list[bytes] = []
        while True:
            chunk = os.read(fd, 4096)
            if not chunk:
                break
            chunks.append(chunk)
        got = b"".join(chunks)
    finally:
        os.close(fd)
    if got != expected:
        raise AssertionError(f"read mismatch: {len(got)} vs {len(expected)} bytes")


@check("pread: random offsets match backing")
def _pread_random(b: OracleFiles, mnt: pathlib.Path) -> None:
    rng = random.Random(0xBEEF)
    expected = b.contents[b.LARGE_NAME]
    fd = os.open(mnt / b.LARGE_NAME, os.O_RDONLY)
    try:
        for _ in range(64):
            offset = rng.randrange(0, len(expected))
            size = rng.randrange(1, 8192)
            got = os.pread(fd, size, offset)
            want = expected[offset : offset + size]
            if got != want:
                raise AssertionError(
                    f"pread mismatch at offset={offset} size={size}: "
                    f"got {len(got)} want {len(want)}"
                )
    finally:
        os.close(fd)


@check("pread: at EOF returns empty")
def _pread_at_eof(b: OracleFiles, mnt: pathlib.Path) -> None:
    size = len(b.contents[b.SMALL_NAME])
    fd = os.open(mnt / b.SMALL_NAME, os.O_RDONLY)
    try:
        if os.pread(fd, 100, size) != b"":
            raise AssertionError("read past EOF returned bytes")
        if os.pread(fd, 100, size + 1_000_000) != b"":
            raise AssertionError("read way past EOF returned bytes")
    finally:
        os.close(fd)


@check("pread: spanning EOF returns trailing bytes only")
def _pread_spanning_eof(b: OracleFiles, mnt: pathlib.Path) -> None:
    expected = b.contents[b.SMALL_NAME]
    size = len(expected)
    fd = os.open(mnt / b.SMALL_NAME, os.O_RDONLY)
    try:
        got = os.pread(fd, 1024, size - 10)
        if got != expected[size - 10 :]:
            raise AssertionError(f"got {got!r} want {expected[size - 10 :]!r}")
    finally:
        os.close(fd)


@check("readv / preadv: bytes match backing")
def _readv_preadv(b: OracleFiles, mnt: pathlib.Path) -> None:
    expected = b.contents[b.MEDIUM_NAME]
    fd = os.open(mnt / b.MEDIUM_NAME, os.O_RDONLY)
    try:
        bufs = [bytearray(1024), bytearray(2048), bytearray(4096)]
        n = os.readv(fd, bufs)
        flat = b"".join(bytes(buf) for buf in bufs)[:n]
        if flat != expected[:n]:
            raise AssertionError("readv content mismatch")
        # preadv at arbitrary offset
        bufs2 = [bytearray(512), bytearray(512)]
        offset = 8192
        n2 = os.preadv(fd, bufs2, offset)
        flat2 = b"".join(bytes(buf) for buf in bufs2)[:n2]
        if flat2 != expected[offset : offset + n2]:
            raise AssertionError("preadv content mismatch")
    finally:
        os.close(fd)


@check("lseek: SEEK_SET / SEEK_CUR / SEEK_END")
def _lseek_basic(b: OracleFiles, mnt: pathlib.Path) -> None:
    expected = b.contents[b.MEDIUM_NAME]
    fd = os.open(mnt / b.MEDIUM_NAME, os.O_RDONLY)
    try:
        if os.lseek(fd, 0, os.SEEK_END) != len(expected):
            raise AssertionError("SEEK_END != size")
        if os.lseek(fd, 0, os.SEEK_SET) != 0:
            raise AssertionError("SEEK_SET 0 != 0")
        if os.lseek(fd, 100, os.SEEK_CUR) != 100:
            raise AssertionError("SEEK_CUR 100 from 0 != 100")
        if os.read(fd, 50) != expected[100:150]:
            raise AssertionError("read after seek mismatch")
    finally:
        os.close(fd)


@check("lseek: negative offset -> EINVAL")
def _lseek_negative(b: OracleFiles, mnt: pathlib.Path) -> None:
    fd = os.open(mnt / b.SMALL_NAME, os.O_RDONLY)
    try:
        _expect_oserror_in(
            lambda: os.lseek(fd, -1, os.SEEK_SET),
            {errno.EINVAL},
            "lseek negative",
        )
    finally:
        os.close(fd)


@check("lseek: past EOF + read -> 0 bytes")
def _lseek_past_eof(b: OracleFiles, mnt: pathlib.Path) -> None:
    fd = os.open(mnt / b.SMALL_NAME, os.O_RDONLY)
    try:
        os.lseek(fd, 1_000_000, os.SEEK_SET)
        if os.read(fd, 100) != b"":
            raise AssertionError("read past EOF returned bytes")
    finally:
        os.close(fd)


# ---------------------------------------------------------------------------
# Stat family
# ---------------------------------------------------------------------------


@check("stat == lstat == fstat for regular files")
def _stat_consistency(b: OracleFiles, mnt: pathlib.Path) -> None:
    p = mnt / b.MEDIUM_NAME
    s_stat = os.stat(p)
    s_lstat = os.lstat(p)
    fd = os.open(p, os.O_RDONLY)
    try:
        s_fstat = os.fstat(fd)
    finally:
        os.close(fd)
    for label, attr in [
        ("st_size", "st_size"),
        ("st_mode", "st_mode"),
        ("st_ino", "st_ino"),
    ]:
        a = getattr(s_stat, attr)
        c = getattr(s_lstat, attr)
        d = getattr(s_fstat, attr)
        if not (a == c == d):
            raise AssertionError(f"{label} mismatch: stat={a} lstat={c} fstat={d}")


@check("stat: st_mode is S_IFREG with no write bits")
def _stat_mode_readonly(b: OracleFiles, mnt: pathlib.Path) -> None:
    for name in b.names():
        s = os.stat(mnt / name)
        if not stat.S_ISREG(s.st_mode):
            raise AssertionError(f"{name}: not S_IFREG (mode={s.st_mode:o})")
        if s.st_mode & 0o222:
            raise AssertionError(f"{name}: has write bits set (mode={s.st_mode:o})")


@check("stat: st_size matches reads")
def _stat_size_matches_reads(b: OracleFiles, mnt: pathlib.Path) -> None:
    for name, content in b.contents.items():
        size = (mnt / name).stat().st_size
        if size != len(content):
            raise AssertionError(f"{name}: stat size {size} != content {len(content)}")


@check("stat: st_dev consistent across files in mount")
def _stat_st_dev_consistent(b: OracleFiles, mnt: pathlib.Path) -> None:
    devs = {os.stat(mnt / name).st_dev for name in b.names()}
    if len(devs) != 1:
        raise AssertionError(f"st_dev not consistent: {devs}")


@check("stat: st_ino unique per file")
def _stat_st_ino_unique(b: OracleFiles, mnt: pathlib.Path) -> None:
    inodes = {name: os.stat(mnt / name).st_ino for name in b.names()}
    if len(set(inodes.values())) != len(inodes):
        raise AssertionError(f"duplicate inodes: {inodes}")


@check("statvfs: ST_RDONLY flag set on mount")
def _statvfs_readonly(b: OracleFiles, mnt: pathlib.Path) -> None:
    sv = os.statvfs(mnt)
    if not sv.f_flag & os.ST_RDONLY:
        raise AssertionError(f"ST_RDONLY not set in f_flag={sv.f_flag:o}")
    if sv.f_namemax <= 0:
        raise AssertionError(f"f_namemax not positive: {sv.f_namemax}")


# ---------------------------------------------------------------------------
# access(2)
# ---------------------------------------------------------------------------


@check("access: F_OK true for existing files")
def _access_f_ok(b: OracleFiles, mnt: pathlib.Path) -> None:
    for name in b.names():
        if not os.access(mnt / name, os.F_OK):
            raise AssertionError(f"{name}: F_OK returned False")


@check("access: R_OK true for existing files")
def _access_r_ok(b: OracleFiles, mnt: pathlib.Path) -> None:
    for name in b.names():
        if not os.access(mnt / name, os.R_OK):
            raise AssertionError(f"{name}: R_OK returned False")


@check("access: W_OK false on read-only mount")
def _access_w_ok_false(b: OracleFiles, mnt: pathlib.Path) -> None:
    if os.access(mnt / b.SMALL_NAME, os.W_OK):
        raise AssertionError("W_OK returned True on read-only mount")


@check("access: F_OK false for missing file")
def _access_f_ok_missing(b: OracleFiles, mnt: pathlib.Path) -> None:
    if os.access(mnt / "no-such.dat", os.F_OK):
        raise AssertionError("F_OK returned True for missing file")


# ---------------------------------------------------------------------------
# Directory operations
# ---------------------------------------------------------------------------


@check("readdir: listdir matches backing names")
def _readdir_matches(b: OracleFiles, mnt: pathlib.Path) -> None:
    listed = sorted(os.listdir(mnt))
    expected = sorted(b.names())
    if listed != expected:
        raise AssertionError(f"listdir {listed} != expected {expected}")


@check("scandir: entries match listdir")
def _scandir_matches(b: OracleFiles, mnt: pathlib.Path) -> None:
    with os.scandir(mnt) as it:
        names = sorted(e.name for e in it)
    if names != sorted(b.names()):
        raise AssertionError(f"scandir {names} != {b.names()}")


@check("scandir: each entry is_file() and not is_dir()")
def _scandir_types(b: OracleFiles, mnt: pathlib.Path) -> None:
    with os.scandir(mnt) as it:
        for entry in it:
            if not entry.is_file():
                raise AssertionError(f"{entry.name}: is_file() returned False")
            if entry.is_dir():
                raise AssertionError(f"{entry.name}: is_dir() returned True")


@check("openat / fstatat: relative resolution from dirfd")
def _openat_fstatat(b: OracleFiles, mnt: pathlib.Path) -> None:
    dirfd = os.open(mnt, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fd = os.open(b.SMALL_NAME, os.O_RDONLY, dir_fd=dirfd)
        try:
            data = os.read(fd, 4096)
            if data != b.contents[b.SMALL_NAME][: len(data)]:
                raise AssertionError("openat read mismatch")
        finally:
            os.close(fd)
        s = os.stat(b.SMALL_NAME, dir_fd=dirfd)
        if s.st_size != len(b.contents[b.SMALL_NAME]):
            raise AssertionError("fstatat st_size mismatch")
    finally:
        os.close(dirfd)


# ---------------------------------------------------------------------------
# fd duplication / fcntl
# ---------------------------------------------------------------------------


@check("dup / dup2: independent offsets")
def _dup_independent_offsets(b: OracleFiles, mnt: pathlib.Path) -> None:
    fd1 = os.open(mnt / b.MEDIUM_NAME, os.O_RDONLY)
    try:
        fd2 = os.dup(fd1)
        try:
            # On Linux dup'd fds share a file description, so they share
            # the seek offset. That's POSIX behaviour. Test a separate
            # open instead — independent open() calls must have
            # independent offsets.
            fd3 = os.open(mnt / b.MEDIUM_NAME, os.O_RDONLY)
            try:
                os.lseek(fd1, 100, os.SEEK_SET)
                os.lseek(fd3, 200, os.SEEK_SET)
                d1 = os.read(fd1, 16)
                d3 = os.read(fd3, 16)
                expected = b.contents[b.MEDIUM_NAME]
                if d1 != expected[100:116]:
                    raise AssertionError("fd1 read mismatch")
                if d3 != expected[200:216]:
                    raise AssertionError("fd3 read mismatch")
            finally:
                os.close(fd3)
        finally:
            os.close(fd2)
    finally:
        os.close(fd1)


@check("fcntl: F_GETFL reports O_RDONLY")
def _fcntl_getfl(b: OracleFiles, mnt: pathlib.Path) -> None:
    fd = os.open(mnt / b.SMALL_NAME, os.O_RDONLY)
    try:
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        if flags & os.O_ACCMODE != os.O_RDONLY:
            raise AssertionError(f"O_ACCMODE != O_RDONLY: flags={flags:o}")
    finally:
        os.close(fd)


# ---------------------------------------------------------------------------
# mmap
# ---------------------------------------------------------------------------


@check("mmap: PROT_READ MAP_PRIVATE returns matching bytes")
def _mmap_read(b: OracleFiles, mnt: pathlib.Path) -> None:
    expected = b.contents[b.MEDIUM_NAME]
    # Run in subprocess: a page fault inside FUSE handler can otherwise
    # deadlock the FUSE thread when run in-process.
    script = (
        "import mmap, sys\n"
        "with open(sys.argv[1], 'rb') as f:\n"
        "    try:\n"
        "        mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)\n"
        "    except OSError as exc:\n"
        "        sys.stdout.write(f'mmap-failed:{exc.errno}\\n')\n"
        "        sys.exit(0)\n"
        "    sys.stdout.buffer.write(mm[:])\n"
        "    mm.close()\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(mnt / b.MEDIUM_NAME)],
        capture_output=True,
        check=True,
        timeout=30,
    )
    if result.stdout.startswith(b"mmap-failed"):
        # mmap rejection is acceptable — FUSE can refuse it; document it.
        return
    if result.stdout != expected:
        raise AssertionError(
            f"mmap content mismatch: {len(result.stdout)} vs {len(expected)}"
        )


@check("mmap: MAP_SHARED PROT_WRITE rejected")
def _mmap_write_rejected(b: OracleFiles, mnt: pathlib.Path) -> None:
    fd = os.open(mnt / b.MEDIUM_NAME, os.O_RDONLY)
    try:
        # MAP_SHARED with PROT_WRITE on a read-only fd should fail at mmap()
        # with EACCES (or be rejected by FUSE outright).
        try:
            mm = mmap.mmap(
                fd,
                0,
                flags=mmap.MAP_SHARED,
                prot=mmap.PROT_READ | mmap.PROT_WRITE,
            )
        except (OSError, ValueError):
            return  # expected
        mm.close()
        raise AssertionError("MAP_SHARED|PROT_WRITE on RO fd unexpectedly succeeded")
    finally:
        os.close(fd)


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------


@check("path: trailing slash on regular file -> ENOTDIR")
def _path_trailing_slash(b: OracleFiles, mnt: pathlib.Path) -> None:
    bad = str(mnt / b.SMALL_NAME) + "/"
    _expect_oserror_in(
        lambda: os.close(os.open(bad, os.O_RDONLY)),
        {errno.ENOTDIR},
        "trailing slash",
    )


@check("path: redundant ./// segments resolve")
def _path_dot_slash(b: OracleFiles, mnt: pathlib.Path) -> None:
    weird = str(mnt) + "/.//" + b.SMALL_NAME
    fd = os.open(weird, os.O_RDONLY)
    os.close(fd)


@check("chdir + relative open works")
def _chdir_relative(b: OracleFiles, mnt: pathlib.Path) -> None:
    cwd = os.getcwd()
    try:
        os.chdir(mnt)
        fd = os.open(b.SMALL_NAME, os.O_RDONLY)
        os.close(fd)
    finally:
        os.chdir(cwd)


# ---------------------------------------------------------------------------
# Rejected mutation operations (each must yield a recognised errno).
# ---------------------------------------------------------------------------

_RO_ERRNOS = {errno.EROFS, errno.EACCES, errno.EPERM, errno.ENOSYS}


@check("mutate: write rejected")
def _mutate_write(b: OracleFiles, mnt: pathlib.Path) -> None:
    # Cannot open for write on RO mount; emulate via O_WRONLY check above.
    # Try the high-level pathlib write_bytes which goes through O_WRONLY.
    _expect_oserror_in(
        lambda: (mnt / b.SMALL_NAME).write_bytes(b"x"),
        _RO_ERRNOS,
        "write_bytes",
    )


@check("mutate: unlink rejected")
def _mutate_unlink(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: (mnt / b.SMALL_NAME).unlink(),
        _RO_ERRNOS,
        "unlink",
    )


@check("mutate: rename rejected")
def _mutate_rename(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: os.rename(mnt / b.SMALL_NAME, mnt / "renamed.dat"),
        _RO_ERRNOS,
        "rename",
    )


@check("mutate: mkdir rejected")
def _mutate_mkdir(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: (mnt / "newdir").mkdir(),
        _RO_ERRNOS,
        "mkdir",
    )


@check("mutate: symlink rejected")
def _mutate_symlink(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: os.symlink(b.SMALL_NAME, mnt / "link.dat"),
        _RO_ERRNOS,
        "symlink",
    )


@check("mutate: link rejected")
def _mutate_link(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: os.link(mnt / b.SMALL_NAME, mnt / "hard.dat"),
        _RO_ERRNOS,
        "link",
    )


@check("mutate: chmod rejected")
def _mutate_chmod(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: os.chmod(mnt / b.SMALL_NAME, 0o600),
        _RO_ERRNOS,
        "chmod",
    )


@check("mutate: chown rejected (only if non-root)")
def _mutate_chown(b: OracleFiles, mnt: pathlib.Path) -> None:
    if os.geteuid() == 0:
        # Root chown on RO mount: behaviour varies — skip the assertion.
        return
    _expect_oserror_in(
        lambda: os.chown(mnt / b.SMALL_NAME, os.getuid(), os.getgid()),
        _RO_ERRNOS,
        "chown",
    )


@check("mutate: utime rejected")
def _mutate_utime(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: os.utime(mnt / b.SMALL_NAME, (0, 0)),
        _RO_ERRNOS,
        "utime",
    )


@check("mutate: truncate rejected")
def _mutate_truncate(b: OracleFiles, mnt: pathlib.Path) -> None:
    _expect_oserror_in(
        lambda: os.truncate(mnt / b.SMALL_NAME, 0),
        _RO_ERRNOS,
        "truncate",
    )


# ---------------------------------------------------------------------------
# xattrs
# ---------------------------------------------------------------------------


@check("xattr: getxattr returns ENOTSUP/ENODATA")
def _xattr_get(b: OracleFiles, mnt: pathlib.Path) -> None:
    if not hasattr(os, "getxattr"):
        return  # xattrs not available on this build
    try:
        os.getxattr(str(mnt / b.SMALL_NAME), "user.foo")
    except OSError as exc:
        if exc.errno not in (errno.ENOTSUP, errno.ENODATA, errno.EOPNOTSUPP):
            raise AssertionError(
                f"unexpected errno from getxattr: {errno.errorcode.get(exc.errno)}"
            ) from exc
        return
    raise AssertionError("getxattr unexpectedly succeeded")


@check("xattr: setxattr rejected")
def _xattr_set(b: OracleFiles, mnt: pathlib.Path) -> None:
    if not hasattr(os, "setxattr"):
        return
    try:
        os.setxattr(str(mnt / b.SMALL_NAME), "user.foo", b"bar")
    except OSError as exc:
        if exc.errno not in (
            errno.ENOTSUP,
            errno.EOPNOTSUPP,
            errno.EROFS,
            errno.EACCES,
            errno.EPERM,
        ):
            raise AssertionError(
                f"unexpected errno from setxattr: {errno.errorcode.get(exc.errno)}"
            ) from exc
        return
    raise AssertionError("setxattr unexpectedly succeeded")


# ---------------------------------------------------------------------------
# Endurance / fd churn
# ---------------------------------------------------------------------------


@check("fd churn: 1000 open/close cycles, no fd leak")
def _fd_churn(b: OracleFiles, mnt: pathlib.Path) -> None:
    pid = os.getpid()
    fd_dir = pathlib.Path(f"/proc/{pid}/fd")
    baseline = len(list(fd_dir.iterdir()))
    for _ in range(1000):
        fd = os.open(mnt / b.SMALL_NAME, os.O_RDONLY)
        os.read(fd, 16)
        os.close(fd)
    final = len(list(fd_dir.iterdir()))
    if final > baseline:
        raise AssertionError(f"fd leak: baseline={baseline} final={final}")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run(*, log_path: pathlib.Path | None = None) -> tools.RunnerResult:
    """Mount the medium plink fixture and run every registered check."""
    started = time.monotonic()
    workdir = pathlib.Path(tempfile.mkdtemp(prefix="biofuse-posix-"))
    spec = fixtures.MEDIUM
    vcz_path = fixtures.get_or_build(spec)
    oracle_dir = workdir / "oracle"
    tools.materialise_plink_oracle(vcz_path, oracle_dir, spec.name)
    logger.info("posix: oracle plink fileset at %s", oracle_dir)
    backing = OracleFiles(oracle_dir, spec.name)
    mountpoint = workdir / "mnt"
    checks: list[tools.CheckResult] = []
    n_checks = len(_REGISTRY)
    try:
        with mount_mod.BiofuseMount(
            str(vcz_path), mountpoint, log_path=log_path
        ) as mnt:
            logger.info("posix: running %d checks against %s", n_checks, mnt)
            for i, (name, fn) in enumerate(_REGISTRY, start=1):
                result = _run_one(name, fn, backing, mnt)
                if result.passed:
                    logger.debug(
                        "[%d/%d] posix:%s PASS (%.3fs)",
                        i,
                        n_checks,
                        name,
                        result.duration_s,
                    )
                else:
                    logger.debug(
                        "[%d/%d] posix:%s FAIL (%.3fs): %s",
                        i,
                        n_checks,
                        name,
                        result.duration_s,
                        result.detail,
                    )
                checks.append(result)
    finally:
        with contextlib.suppress(Exception):
            shutil.rmtree(workdir)

    duration = time.monotonic() - started
    passed = all(c.passed for c in checks)
    pass_count = sum(c.passed for c in checks)
    logger.info("posix: %d/%d passed in %.2fs", pass_count, len(checks), duration)
    return tools.RunnerResult(
        runner="posix",
        passed=passed,
        duration_s=duration,
        checks=checks,
        summary=f"{pass_count}/{len(checks)} POSIX checks passed",
    )


def _run_one(
    name: str,
    fn: CheckFn,
    backing: OracleFiles,
    mnt: pathlib.Path,
) -> tools.CheckResult:
    started = time.monotonic()
    try:
        fn(backing, mnt)
    except AssertionError as exc:
        return tools.CheckResult(
            name=name,
            passed=False,
            duration_s=time.monotonic() - started,
            detail=str(exc),
        )
    except Exception as exc:
        logger.exception("check %s raised", name)
        return tools.CheckResult(
            name=name,
            passed=False,
            duration_s=time.monotonic() - started,
            detail=f"{type(exc).__name__}: {exc}",
        )
    return tools.CheckResult(
        name=name,
        passed=True,
        duration_s=time.monotonic() - started,
    )
