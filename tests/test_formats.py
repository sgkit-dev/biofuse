"""Tests for biofuse.formats — the per-format spec table.

Pins the duck-typed contract that ``encoder_server`` / ``encoder_ops``
rely on: each spec produces the expected static sidecar bytes for the
shared :class:`VczReader`, and each ``encoder_factory`` yields an
encoder whose ``total_size`` reflects the same selection.
"""

import sqlite3
import tempfile

import pytest
from vcztools import bgen as vcztools_bgen
from vcztools import plink as vcztools_plink
from vcztools.cli import make_reader

from biofuse import formats


@pytest.fixture
def fx_reader(fx_small_vcz):
    return make_reader(str(fx_small_vcz.path))


@pytest.fixture
def fx_haploid_reader(fx_haploid_vcz):
    return make_reader(str(fx_haploid_vcz.path))


@pytest.fixture
def fx_mixed_ploidy_reader(fx_mixed_ploidy_vcz):
    return make_reader(str(fx_mixed_ploidy_vcz.path))


class TestPlinkSpec:
    def test_identifiers(self):
        assert formats.PLINK_SPEC.name == "plink"
        assert formats.PLINK_SPEC.streaming_suffix == ".bed"
        assert formats.PLINK_SPEC.streaming_kind == "bed"
        assert formats.PLINK_SPEC.static_suffixes == (".bim", ".fam")

    def test_static_files_match_generators(self, fx_reader):
        static = formats.PLINK_SPEC.build_static_files(fx_reader)
        assert set(static) == {".bim", ".fam"}
        assert static[".bim"] == vcztools_plink.generate_bim(fx_reader).encode("utf-8")
        assert static[".fam"] == vcztools_plink.generate_fam(fx_reader).encode("utf-8")

    def test_encoder_total_size(self, fx_reader, fx_small_vcz):
        with formats.PLINK_SPEC.encoder_factory(fx_reader) as encoder:
            bytes_per_variant = (fx_small_vcz.num_samples + 3) // 4
            expected = 3 + fx_small_vcz.num_variants * bytes_per_variant
            assert encoder.total_size == expected


class TestBgenSpec:
    def test_identifiers(self):
        assert formats.BGEN_SPEC.name == "bgen"
        assert formats.BGEN_SPEC.streaming_suffix == ".bgen"
        assert formats.BGEN_SPEC.streaming_kind == "bgen"
        assert formats.BGEN_SPEC.static_suffixes == (".sample", ".bgen.bgi")

    def test_static_files_keys_match_suffixes(self, fx_reader):
        static = formats.BGEN_SPEC.build_static_files(fx_reader)
        assert set(static) == {".sample", ".bgen.bgi"}

    def test_sample_matches_generator(self, fx_reader):
        static = formats.BGEN_SPEC.build_static_files(fx_reader)
        assert static[".sample"] == vcztools_bgen.generate_sample(fx_reader).encode(
            "utf-8"
        )

    def test_bgi_opens_as_sqlite(self, fx_reader, tmp_path, fx_small_vcz):
        static = formats.BGEN_SPEC.build_static_files(fx_reader)
        bgi_path = tmp_path / "small.bgen.bgi"
        bgi_path.write_bytes(static[".bgen.bgi"])
        conn = sqlite3.connect(str(bgi_path))
        try:
            row_count = conn.execute("SELECT COUNT(*) FROM Variant").fetchone()[0]
        finally:
            conn.close()
        assert row_count == fx_small_vcz.num_variants

    def test_bgi_schema_columns(self, fx_reader, tmp_path):
        static = formats.BGEN_SPEC.build_static_files(fx_reader)
        bgi_path = tmp_path / "small.bgen.bgi"
        bgi_path.write_bytes(static[".bgen.bgi"])
        conn = sqlite3.connect(str(bgi_path))
        try:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(Variant)")}
        finally:
            conn.close()
        assert {
            "chromosome",
            "position",
            "rsid",
            "number_of_alleles",
            "allele1",
            "allele2",
            "file_start_position",
            "size_in_bytes",
        } <= columns

    def test_tempfile_cleaned_up(self, fx_reader, tmp_path, monkeypatch):
        monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
        formats.BGEN_SPEC.build_static_files(fx_reader)
        leftovers = list(tmp_path.glob("biofuse-bgen-*"))
        assert leftovers == [], f"left behind: {leftovers}"

    def test_encoder_total_size_is_fixed_size_layout(self, fx_reader):
        with formats.BGEN_SPEC.encoder_factory(fx_reader) as encoder:
            assert encoder.total_size == (
                encoder.prefix_size + encoder.num_variants * encoder.bytes_per_variant
            )
            assert encoder.total_size > 0


class TestPlinkSpecHaploid:
    """Pure-haploid VCZ → ``call_genotype.shape == (V, S, 1)``.

    The PLINK BED format is diploid-only and the vcztools C kernel
    rejects non-diploid input outright (``ValueError`` from
    ``_vcztools.encode_plink``). Static sidecars are ploidy-agnostic
    and build successfully — the failure is at first ``.bed`` read.
    """

    def test_static_files_build(self, fx_haploid_reader, fx_haploid_vcz):
        static = formats.PLINK_SPEC.build_static_files(fx_haploid_reader)
        assert set(static) == {".bim", ".fam"}
        assert len(static[".bim"].splitlines()) == fx_haploid_vcz.num_biallelic_sites
        assert len(static[".fam"].splitlines()) == fx_haploid_vcz.num_samples

    def test_encoder_read_raises_value_error(self, fx_haploid_reader):
        with formats.PLINK_SPEC.encoder_factory(fx_haploid_reader) as encoder:
            with pytest.raises(ValueError, match="diploid"):
                encoder.read(0, encoder.total_size)


class TestPlinkSpecMixedPloidy:
    """Diploid-shaped genotype array with -2 in slot 1 for half the samples.

    The vcztools PLINK C kernel encodes ``b == -2`` as a homozygous
    call for ``a`` — lossy but not an error, which is the intentional
    PLINK 1 contract.
    """

    def test_encoder_read_succeeds(self, fx_mixed_ploidy_reader, fx_mixed_ploidy_vcz):
        with formats.PLINK_SPEC.encoder_factory(fx_mixed_ploidy_reader) as encoder:
            bytes_per_variant = (fx_mixed_ploidy_vcz.num_samples + 3) // 4
            expected_size = 3 + fx_mixed_ploidy_vcz.num_variants * bytes_per_variant
            assert encoder.total_size == expected_size
            data = encoder.read(0, encoder.total_size)
            assert len(data) == expected_size


class TestBgenSpecHaploid:
    """Pure-haploid VCZ. BgenEncoder promotes ``(V, S, 1)`` internally and
    emits a valid haploid BGEN payload — fully supported."""

    def test_static_files_build(self, fx_haploid_reader, fx_haploid_vcz, tmp_path):
        static = formats.BGEN_SPEC.build_static_files(fx_haploid_reader)
        assert set(static) == {".sample", ".bgen.bgi"}
        sample_lines = static[".sample"].decode("utf-8").splitlines()
        # Header + column-type row + one row per sample.
        assert len(sample_lines) == fx_haploid_vcz.num_samples + 2

        bgi_path = tmp_path / "haploid.bgen.bgi"
        bgi_path.write_bytes(static[".bgen.bgi"])
        conn = sqlite3.connect(str(bgi_path))
        try:
            count = conn.execute("SELECT COUNT(*) FROM Variant").fetchone()[0]
        finally:
            conn.close()
        assert count == fx_haploid_vcz.num_variants

    def test_encoder_read_matches_in_process(self, fx_haploid_reader, fx_haploid_vcz):
        with formats.BGEN_SPEC.encoder_factory(fx_haploid_reader) as encoder:
            data_spec = encoder.read(0, encoder.total_size)
        in_process_reader = make_reader(str(fx_haploid_vcz.path))
        with vcztools_bgen.BgenEncoder(in_process_reader) as ref:
            data_ref = ref.read(0, ref.total_size)
        assert data_spec == data_ref


class TestBgenSpecMixedPloidy:
    """Diploid-shaped genotype array with -2 in slot 1 for half the samples.

    The fixed-size BgenEncoder builds and produces an index (the ``.bgen.bgi``
    sidecar depends only on per-variant offsets) but raises
    ``NotImplementedError`` on the first chunk read — pinning the failure
    mode users will hit when a biofuse BGEN mount spans X / Y / MT.
    """

    def test_static_files_build_succeeds(self, fx_mixed_ploidy_reader):
        static = formats.BGEN_SPEC.build_static_files(fx_mixed_ploidy_reader)
        assert set(static) == {".sample", ".bgen.bgi"}
        assert len(static[".bgen.bgi"]) > 0

    def test_encoder_read_raises_not_implemented(self, fx_mixed_ploidy_reader):
        with formats.BGEN_SPEC.encoder_factory(fx_mixed_ploidy_reader) as encoder:
            with pytest.raises(NotImplementedError, match="mixed ploidy"):
                encoder.read(0, encoder.total_size)


class TestSpecsRegistry:
    def test_specs_dict_has_both_entries(self):
        assert formats.SPECS == {"plink": formats.PLINK_SPEC, "bgen": formats.BGEN_SPEC}
