"""Tests for PlinkSource."""

import pathlib
import tempfile
from unittest.mock import patch

import pytest
from vcztools.cli import make_reader
from vcztools.plink import write_plink

from biofuse import plink_source


class TestDefaultBasename:
    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("sample.vcz", "sample"),
            ("/tmp/data/sample.vcz", "sample"),
            ("sample.vcz.zip", "sample"),
            ("a.b.c.vcz", "a"),
            ("noext", "noext"),
        ],
    )
    def test_strips_all_extensions(self, path, expected):
        assert plink_source._default_basename(path) == expected


class TestLifecycle:
    def test_produces_full_fileset(self, fx_small_vcz):
        src = plink_source.PlinkSource(fx_small_vcz.path)
        backing = src.open()
        try:
            names = sorted(p.name for p in backing.iterdir())
            assert names == ["small.bed", "small.bim", "small.fam"]
            assert (backing / "small.bed").stat().st_size > 0
            assert (backing / "small.bim").stat().st_size > 0
            assert (backing / "small.fam").stat().st_size > 0
        finally:
            src.close()
        assert not backing.exists()

    def test_context_manager(self, fx_small_vcz):
        with plink_source.PlinkSource(fx_small_vcz.path) as backing:
            assert (backing / "small.bed").exists()
        assert not backing.exists()

    def test_close_is_idempotent(self, fx_small_vcz):
        src = plink_source.PlinkSource(fx_small_vcz.path)
        src.open()
        src.close()
        src.close()

    def test_double_open_raises(self, fx_small_vcz):
        src = plink_source.PlinkSource(fx_small_vcz.path)
        try:
            src.open()
            with pytest.raises(RuntimeError):
                src.open()
        finally:
            src.close()

    def test_explicit_basename(self, fx_small_vcz):
        with plink_source.PlinkSource(fx_small_vcz.path, basename="custom") as backing:
            names = sorted(p.name for p in backing.iterdir())
            assert names == ["custom.bed", "custom.bim", "custom.fam"]


class TestErrorHandling:
    def test_invalid_vcz_path_cleans_up(self, tmp_path):
        bogus = tmp_path / "not-a-vcz"
        bogus.mkdir()
        src = plink_source.PlinkSource(bogus)
        before_tmps = _count_temp_dirs()
        with pytest.raises((FileNotFoundError, ValueError, OSError), match=".*"):
            src.open()
        after_tmps = _count_temp_dirs()
        assert after_tmps == before_tmps

    def test_write_plink_failure_cleans_up(self, fx_small_vcz):
        before_tmps = _count_temp_dirs()
        src = plink_source.PlinkSource(fx_small_vcz.path)
        with patch(
            "biofuse.plink_source.vcztools_plink.write_plink",
            side_effect=RuntimeError("boom"),
        ):
            with pytest.raises(RuntimeError, match="boom"):
                src.open()
        after_tmps = _count_temp_dirs()
        assert after_tmps == before_tmps


class TestGoldenParity:
    """The materialised files must match a direct vcztools.plink.write_plink call."""

    def test_files_byte_for_byte(self, fx_small_vcz, tmp_path):
        golden_prefix = tmp_path / "golden"
        write_plink(make_reader(str(fx_small_vcz.path)), golden_prefix)

        with plink_source.PlinkSource(fx_small_vcz.path, basename="golden") as backing:
            for suffix in (".bed", ".bim", ".fam"):
                got = (backing / f"golden{suffix}").read_bytes()
                expected = pathlib.Path(str(golden_prefix) + suffix).read_bytes()
                assert got == expected


def _count_temp_dirs() -> int:
    """Count biofuse-prefixed temp directories under the system tmp."""
    return len(list(pathlib.Path(tempfile.gettempdir()).glob("biofuse-plink-*")))
