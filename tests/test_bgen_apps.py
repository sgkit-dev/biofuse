"""End-to-end tests for BGEN mounts.

Mirrors :mod:`test_plink_apps`: mount a VCZ as a BGEN fileset via the
real encoder-server subprocess, then read the mounted files. Where
``plink2`` is on PATH, ``TestPlinkTwo`` also exercises the mount as a
BGEN reader and compares its output to a side-by-side copy of the
encoder's bytes.
"""

import contextlib
import errno
import os
import pathlib
import shutil
import sqlite3

import pytest
import trio
from vcztools.bgen import BgenEncoder
from vcztools.cli import make_reader

from biofuse import access_log, encoder_client, encoder_ops, formats, fuse_adapter

PLINK2 = shutil.which("plink2")

needs_plink2 = pytest.mark.skipif(PLINK2 is None, reason="plink2 not installed")


async def _wait_for_mount(mnt: pathlib.Path, timeout: float = 5.0) -> None:
    """Block until ``mnt`` is a live FUSE mount.

    ``os.path.ismount`` makes a sync ``lstat`` call into the FUSE mount,
    which the kernel forwards to the userspace pyfuse3 daemon — the
    same trio thread that's calling ismount. A direct call would
    deadlock; ``trio.to_thread.run_sync`` runs the syscall on a worker
    thread so the trio loop stays free to serve the FUSE request.
    """
    deadline = trio.current_time() + timeout
    while trio.current_time() < deadline:
        live = await trio.to_thread.run_sync(os.path.ismount, str(mnt))
        if live:
            return
        await trio.sleep(0.02)
    raise RuntimeError(f"mountpoint {mnt} not live after {timeout}s")


@pytest.fixture
async def fx_mounted_bgen(tmp_path, fx_medium_vcz):
    """Mount the medium VCZ as a BGEN fileset via biofuse.

    Yields ``(mnt, basename, expected_bytes, log)`` where ``expected_bytes``
    is the full streaming-file bytes the encoder would emit (computed
    once via an in-process ``BgenEncoder`` on a fresh reader). We do
    not compare against ``vcztools.bgen.write_bgen`` output because
    ``write_bgen`` defaults to compressed BGEN (smaller, non-fixed-size)
    while biofuse's encoder always uses level 0 (fixed-size for random
    access).
    """
    mnt = tmp_path / "mnt"
    mnt.mkdir()
    expected = _encoder_bytes(fx_medium_vcz.path)

    log = access_log.AccessLogger()
    sock_path = tmp_path / "bgen.sock"
    async with await encoder_client.EncoderClient.start(
        str(fx_medium_vcz.path), sock_path, formats.BGEN_SPEC
    ) as client:
        ops = encoder_ops.EncoderOps(
            client, "medium", formats.BGEN_SPEC, access_logger=log
        )
        async with fuse_adapter.mount(ops, str(mnt)):
            await _wait_for_mount(mnt)
            yield mnt, "medium", expected, log


def _encoder_bytes(vcz_path: pathlib.Path) -> bytes:
    reader = make_reader(str(vcz_path))
    with BgenEncoder(reader) as enc:
        return enc.read(0, enc.total_size)


async def _arun(cmd) -> None:
    await trio.run_process(cmd, capture_stdout=True, capture_stderr=True, check=True)


@contextlib.asynccontextmanager
async def _mount_bgen(tmp_path, vcz):
    """Mount ``vcz`` as a BGEN fileset; yield ``(mnt, basename)``."""
    mnt = tmp_path / "mnt"
    mnt.mkdir()
    basename = vcz.path.stem
    sock_path = tmp_path / "bgen.sock"
    async with await encoder_client.EncoderClient.start(
        str(vcz.path), sock_path, formats.BGEN_SPEC
    ) as client:
        ops = encoder_ops.EncoderOps(client, basename, formats.BGEN_SPEC)
        async with fuse_adapter.mount(ops, str(mnt)):
            await _wait_for_mount(mnt)
            yield mnt, basename


class TestBgenBytes:
    async def test_full_bgen_matches_encoder(self, fx_mounted_bgen):
        mnt, basename, expected, _ = fx_mounted_bgen
        data = await trio.to_thread.run_sync((mnt / f"{basename}.bgen").read_bytes)
        assert data == expected

    async def test_sample_text_well_formed(self, fx_mounted_bgen):
        mnt, basename, _, _ = fx_mounted_bgen
        text = await trio.to_thread.run_sync((mnt / f"{basename}.sample").read_text)
        lines = text.splitlines()
        # Header (ID_1 ID_2 missing), column-type row (0 0 0), then one row
        # per sample.
        assert lines[0].split() == ["ID_1", "ID_2", "missing"]
        assert lines[1].split() == ["0", "0", "0"]
        assert len(lines) >= 3

    async def test_bgi_parses_as_sqlite(self, fx_mounted_bgen, fx_medium_vcz):
        mnt, basename, _, _ = fx_mounted_bgen
        bgi_path = mnt / f"{basename}.bgen.bgi"
        count = await trio.to_thread.run_sync(_bgi_variant_count, bgi_path)
        assert count == fx_medium_vcz.num_variants


class TestBgenSeekable:
    async def test_random_pread(self, fx_mounted_bgen):
        """Random-offset reads from the mounted .bgen match the encoder's
        full-bytes view at the same window."""
        mnt, basename, expected, _ = fx_mounted_bgen
        bgen_path = mnt / f"{basename}.bgen"
        # Every syscall against the FUSE mount must run on a worker
        # thread; the trio thread itself is busy serving FUSE requests.
        # See test_plink_apps._wait_for_mount for the same rationale.
        for off, size in [
            (0, 32),
            (len(expected) // 3, 1024),
            (len(expected) - 17, 100),
        ]:
            got = await trio.to_thread.run_sync(_pread_sync, bgen_path, off, size)
            assert got == expected[off : off + size]


def _pread_sync(path: pathlib.Path, off: int, size: int) -> bytes:
    with path.open("rb") as f:
        f.seek(off)
        return f.read(size)


def _bgi_variant_count(bgi_path: pathlib.Path) -> int:
    # ``mode=ro`` keeps sqlite from creating a -journal sidecar; biofuse
    # mounts are read-only so any write attempt would surface as EROFS.
    conn = sqlite3.connect(f"file:{bgi_path}?mode=ro", uri=True)
    try:
        return conn.execute("SELECT COUNT(*) FROM Variant").fetchone()[0]
    finally:
        conn.close()


@needs_plink2
class TestPlinkTwo:
    async def test_freq_matches_plain_bgen(self, fx_mounted_bgen, tmp_path):
        """``plink2 --bgen`` on the mount must produce the same ``.afreq``
        as the same invocation against a side-by-side copy of the
        encoder's bytes. Pins that the mounted ``.bgen`` is a valid BGEN
        reader input and that random-access reads (plink2 scans the
        file) deliver identical bytes to a plain on-disk file."""
        mnt, basename, expected, _ = fx_mounted_bgen
        plain = tmp_path / "plain.bgen"
        plain.write_bytes(expected)
        # plink2 --bgen needs a matching .sample alongside the .bgen.
        await trio.to_thread.run_sync(
            shutil.copyfile,
            mnt / f"{basename}.sample",
            tmp_path / "plain.sample",
        )
        out_mnt = tmp_path / "p2_freq_mnt"
        out_plain = tmp_path / "p2_freq_plain"
        # ``ref-first``: vcztools emits BGEN with allele 0 = REF
        # (matching the VCZ ``variant_allele`` order).
        await _arun(
            [
                PLINK2,
                "--bgen",
                str(mnt / f"{basename}.bgen"),
                "ref-first",
                "--sample",
                str(mnt / f"{basename}.sample"),
                "--freq",
                "--out",
                str(out_mnt),
            ]
        )
        await _arun(
            [
                PLINK2,
                "--bgen",
                str(plain),
                "ref-first",
                "--sample",
                str(tmp_path / "plain.sample"),
                "--freq",
                "--out",
                str(out_plain),
            ]
        )
        assert (
            out_mnt.with_suffix(".afreq").read_bytes()
            == out_plain.with_suffix(".afreq").read_bytes()
        )


class TestStatfs:
    """End-to-end check that ``statvfs(2)`` against the mount returns
    sensible values rather than ENOSYS.
    """

    async def test_statfs_via_os_statvfs(self, fx_mounted_bgen):
        mnt, _, _, _ = fx_mounted_bgen
        info = await trio.to_thread.run_sync(os.statvfs, str(mnt))
        assert info.f_bsize > 0
        assert info.f_blocks > 0
        assert info.f_bavail == 0
        assert info.f_ffree == 0
        assert info.f_files == 3
        assert info.f_namemax >= 255


class TestHaploidAndMixedPloidy:
    """Pins the mount-level behaviour for non-diploid VCZ inputs.

    See ``tests/test_formats.py::TestBgenSpecHaploid`` and
    ``TestBgenSpecMixedPloidy`` for the encoder-layer contracts these
    end-to-end tests exercise.
    """

    async def test_haploid_bgen_matches_encoder(self, tmp_path, fx_haploid_vcz):
        """Pure haploid is fully supported by BgenEncoder; the mounted
        ``.bgen`` bytes must match the in-process encoder's output."""
        expected = _encoder_bytes(fx_haploid_vcz.path)
        async with _mount_bgen(tmp_path, fx_haploid_vcz) as (mnt, basename):
            data = await trio.to_thread.run_sync((mnt / f"{basename}.bgen").read_bytes)
        assert data == expected

    @needs_plink2
    async def test_haploid_bgen_plink2_freq(self, tmp_path, fx_haploid_vcz):
        """``plink2 --bgen`` reads the mounted haploid view end-to-end."""
        async with _mount_bgen(tmp_path, fx_haploid_vcz) as (mnt, basename):
            out = tmp_path / "p2_freq_hap"
            await _arun(
                [
                    PLINK2,
                    "--bgen",
                    str(mnt / f"{basename}.bgen"),
                    "ref-first",
                    "--sample",
                    str(mnt / f"{basename}.sample"),
                    "--freq",
                    "--out",
                    str(out),
                ]
            )
            assert out.with_suffix(".afreq").exists()

    async def test_mixed_ploidy_bgen_read_raises_eio(
        self, tmp_path, fx_mixed_ploidy_vcz
    ):
        """The fixed-size BGEN encoder cannot represent mixed ploidy.
        The mount and the static sidecars (``.sample``, ``.bgen.bgi``)
        still serve, but the first ``.bgen`` read fails with EIO."""
        async with _mount_bgen(tmp_path, fx_mixed_ploidy_vcz) as (mnt, basename):
            await trio.to_thread.run_sync((mnt / f"{basename}.sample").read_bytes)
            await trio.to_thread.run_sync((mnt / f"{basename}.bgen.bgi").read_bytes)
            with pytest.raises(OSError, match="Input/output error") as excinfo:
                await trio.to_thread.run_sync((mnt / f"{basename}.bgen").read_bytes)
            assert excinfo.value.errno == errno.EIO


class TestAccessTrace:
    """Captures access patterns for inspection in phase-2 design."""

    async def test_read_produces_traces(self, fx_mounted_bgen):
        mnt, basename, _, log = fx_mounted_bgen
        await trio.to_thread.run_sync((mnt / f"{basename}.bgen").read_bytes)
        await trio.to_thread.run_sync((mnt / f"{basename}.sample").read_bytes)
        await trio.to_thread.run_sync((mnt / f"{basename}.bgen.bgi").read_bytes)

        records = log.records
        assert len(records) > 0
        bgen_records = [r for r in records if r.path.endswith(".bgen")]
        sample_records = [r for r in records if r.path.endswith(".sample")]
        bgi_records = [r for r in records if r.path.endswith(".bgen.bgi")]
        assert len(bgen_records) > 0, "expected reads against .bgen"
        assert len(sample_records) > 0, "expected reads against .sample"
        assert len(bgi_records) > 0, "expected reads against .bgen.bgi"

        if bgen_records:
            min_off = min(r.offset for r in bgen_records)
            max_end = max(r.offset + r.size for r in bgen_records)
            assert min_off == 0
            assert max_end > 0
