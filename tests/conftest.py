"""Session-scoped VCZ fixtures for the biofuse test suite.

Fixtures are built once per session via msprime + bio2zarr and held under a
session-scoped tmp directory. Tests that need to mutate fixture data must
copy first.

Naming convention: every pytest fixture in this suite uses the ``fx_``
prefix to distinguish fixtures from plain identifiers in production code,
matching the convention used in ``vcztools/tests/conftest.py``.
"""

import pathlib

import pytest

from tests import helpers


@pytest.fixture(scope="session")
def fx_session_dir(tmp_path_factory) -> pathlib.Path:
    return tmp_path_factory.mktemp("biofuse_fixtures")


@pytest.fixture(scope="session")
def fx_small_vcz(fx_session_dir) -> helpers.VczFixture:
    """Tiny VCZ: ~10 diploid samples, multiple variant chunks for boundary testing."""
    return helpers.simulate_vcz(
        out_dir=fx_session_dir / "small",
        num_diploid_samples=10,
        sequence_length=10_000,
        mutation_rate=1e-3,
        variants_chunk_size=7,
        samples_chunk_size=10,
        name="small",
        seed=11,
    )


@pytest.fixture(scope="session")
def fx_medium_vcz(fx_session_dir) -> helpers.VczFixture:
    """Mid-sized VCZ: ~50 diploid samples × hundreds of variants for app tests."""
    return helpers.simulate_vcz(
        out_dir=fx_session_dir / "medium",
        num_diploid_samples=50,
        sequence_length=200_000,
        mutation_rate=1e-4,
        variants_chunk_size=50,
        samples_chunk_size=25,
        name="medium",
        seed=23,
    )


@pytest.fixture(scope="session")
def fx_singleton_vcz(fx_session_dir) -> helpers.VczFixture:
    """Single-variant VCZ. Tiny edge case: chunk count 1, file size minimal."""
    return helpers.simulate_vcz(
        out_dir=fx_session_dir / "singleton",
        num_diploid_samples=4,
        sequence_length=1000,
        mutation_rate=2e-3,
        variants_chunk_size=10,
        samples_chunk_size=4,
        name="singleton",
        seed=37,
    )
