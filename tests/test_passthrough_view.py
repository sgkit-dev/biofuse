"""Unit tests for PassthroughDirectoryView."""

import pathlib
import threading

import pytest
from vcztools.cli import make_reader
from vcztools.plink import write_plink

from biofuse import access_log, passthrough_view


@pytest.fixture
def fx_dir_with_files(tmp_path):
    """A flat directory with three files of distinct content."""
    (tmp_path / "alpha.bed").write_bytes(bytes(range(256)))
    (tmp_path / "alpha.bim").write_text("chr1\trs1\t0\t100\tA\tG\n")
    (tmp_path / "alpha.fam").write_text("0\tS1\t0\t0\t0\t-9\n")
    return tmp_path


@pytest.fixture
def fx_view(fx_dir_with_files):
    v = passthrough_view.PassthroughDirectoryView(fx_dir_with_files)
    yield v
    v.close()


class TestConstruction:
    def test_missing_directory_raises(self, tmp_path):
        missing = tmp_path / "nope"
        with pytest.raises((FileNotFoundError, NotADirectoryError)):
            passthrough_view.PassthroughDirectoryView(missing)

    def test_file_path_raises(self, tmp_path):
        f = tmp_path / "f"
        f.write_text("x")
        with pytest.raises(NotADirectoryError):
            passthrough_view.PassthroughDirectoryView(f)

    def test_empty_directory_is_valid(self, tmp_path):
        v = passthrough_view.PassthroughDirectoryView(tmp_path)
        assert v.list() == []

    def test_subdirectories_are_skipped(self, tmp_path):
        (tmp_path / "f.bed").write_bytes(b"x")
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "nested").write_bytes(b"y")
        v = passthrough_view.PassthroughDirectoryView(tmp_path)
        assert [e.name for e in v.list()] == ["f.bed"]


class TestListAndStat:
    def test_list_returns_all_files(self, fx_view):
        names = sorted(e.name for e in fx_view.list())
        assert names == ["alpha.bed", "alpha.bim", "alpha.fam"]

    def test_stat_reports_size(self, fx_view):
        assert fx_view.stat("alpha.bed").size == 256

    def test_stat_reports_text_size(self, fx_view, fx_dir_with_files):
        bim_path = fx_dir_with_files / "alpha.bim"
        assert fx_view.stat("alpha.bim").size == len(bim_path.read_bytes())

    def test_stat_unknown_raises(self, fx_view):
        with pytest.raises(FileNotFoundError):
            fx_view.stat("missing.bed")


class TestOpenReadRelease:
    def test_full_read(self, fx_view):
        fh = fx_view.open("alpha.bed")
        data = fx_view.read(fh, 0, 1024)
        fx_view.release(fh)
        assert data == bytes(range(256))

    def test_partial_read_at_offset(self, fx_view):
        fh = fx_view.open("alpha.bed")
        try:
            assert fx_view.read(fh, 100, 50) == bytes(range(100, 150))
        finally:
            fx_view.release(fh)

    def test_read_past_eof(self, fx_view):
        fh = fx_view.open("alpha.bed")
        try:
            assert fx_view.read(fh, 256, 100) == b""
            assert fx_view.read(fh, 1_000_000, 100) == b""
        finally:
            fx_view.release(fh)

    def test_read_truncates_at_eof(self, fx_view):
        fh = fx_view.open("alpha.bed")
        try:
            assert fx_view.read(fh, 250, 100) == bytes(range(250, 256))
        finally:
            fx_view.release(fh)

    def test_zero_size_read(self, fx_view):
        fh = fx_view.open("alpha.bed")
        try:
            assert fx_view.read(fh, 0, 0) == b""
        finally:
            fx_view.release(fh)

    def test_negative_size_returns_empty(self, fx_view):
        fh = fx_view.open("alpha.bed")
        try:
            assert fx_view.read(fh, 0, -10) == b""
        finally:
            fx_view.release(fh)

    def test_open_unknown_raises(self, fx_view):
        with pytest.raises(FileNotFoundError):
            fx_view.open("missing.bed")

    def test_read_on_unknown_handle_raises(self, fx_view):
        with pytest.raises(OSError, match=".*"):
            fx_view.read(99999, 0, 10)

    def test_read_after_release_raises(self, fx_view):
        fh = fx_view.open("alpha.bed")
        fx_view.release(fh)
        with pytest.raises(OSError, match=".*"):
            fx_view.read(fh, 0, 10)

    def test_release_unknown_handle_silently_ok(self, fx_view):
        fx_view.release(99999)

    def test_release_is_idempotent(self, fx_view):
        fh = fx_view.open("alpha.bed")
        fx_view.release(fh)
        fx_view.release(fh)


class TestConcurrentHandles:
    def test_two_handles_independent_reads(self, fx_view):
        fh1 = fx_view.open("alpha.bed")
        fh2 = fx_view.open("alpha.bed")
        try:
            assert fx_view.read(fh1, 0, 10) == bytes(range(10))
            assert fx_view.read(fh2, 200, 10) == bytes(range(200, 210))
            assert fx_view.read(fh1, 50, 10) == bytes(range(50, 60))
        finally:
            fx_view.release(fh1)
            fx_view.release(fh2)

    def test_reads_under_thread_pool(self, fx_view):
        fh = fx_view.open("alpha.bed")
        try:
            n_threads = 16
            n_reads = 50
            errors: list[str] = []

            def worker(start: int):
                for i in range(n_reads):
                    offset = (start + i * 7) % 256
                    expected = bytes(range(offset, min(offset + 4, 256)))
                    got = fx_view.read(fh, offset, 4)
                    if got != expected:
                        errors.append(f"offset {offset}: {got!r} != {expected!r}")

            threads = [
                threading.Thread(target=worker, args=(t,)) for t in range(n_threads)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            assert errors == []
        finally:
            fx_view.release(fh)


class TestAccessLogger:
    def test_records_each_read(self, fx_dir_with_files):
        log = access_log.AccessLogger()
        with passthrough_view.PassthroughDirectoryView(
            fx_dir_with_files, access_logger=log
        ) as v:
            fh = v.open("alpha.bed")
            v.read(fh, 0, 10)
            v.read(fh, 100, 50)
            v.release(fh)
        records = log.records
        assert [(r.path, r.offset, r.size) for r in records] == [
            ("alpha.bed", 0, 10),
            ("alpha.bed", 100, 50),
        ]

    def test_records_actual_bytes_returned(self, fx_dir_with_files):
        log = access_log.AccessLogger()
        with passthrough_view.PassthroughDirectoryView(
            fx_dir_with_files, access_logger=log
        ) as v:
            fh = v.open("alpha.bed")
            v.read(fh, 250, 100)  # only 6 bytes available
            v.release(fh)
        rec = log.records[0]
        assert rec.size == 6


class TestCloseLifecycle:
    def test_close_releases_open_handles(self, fx_dir_with_files):
        v = passthrough_view.PassthroughDirectoryView(fx_dir_with_files)
        fh1 = v.open("alpha.bed")
        fh2 = v.open("alpha.bim")
        v.close()
        with pytest.raises(OSError, match=".*"):
            v.read(fh1, 0, 10)
        with pytest.raises(OSError, match=".*"):
            v.read(fh2, 0, 10)

    def test_double_close_ok(self, fx_dir_with_files):
        v = passthrough_view.PassthroughDirectoryView(fx_dir_with_files)
        v.close()
        v.close()


class TestVczGoldenParity:
    """Tests that the passthrough view exposes vcztools-generated bytes verbatim."""

    def test_passthrough_matches_disk_bytes(self, tmp_path, fx_small_vcz):
        out_prefix = tmp_path / fx_small_vcz.path.stem
        reader = make_reader(fx_small_vcz.path)
        write_plink(reader, out_prefix)

        v = passthrough_view.PassthroughDirectoryView(tmp_path)
        try:
            for suffix in (".bed", ".bim", ".fam"):
                disk_path = pathlib.Path(str(out_prefix) + suffix)
                fh = v.open(disk_path.name)
                got = v.read(fh, 0, disk_path.stat().st_size + 1024)
                v.release(fh)
                assert got == disk_path.read_bytes()
        finally:
            v.close()
