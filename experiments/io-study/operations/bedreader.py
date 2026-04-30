"""bed-reader operations.

bed-reader is a Python library, not a CLI. The harness invokes the
runner script under ``scripts/bedreader_runner.py``, which dispatches
on the ``--op`` argument. Four ops fingerprint distinct access shapes:

- ``full_scan`` — read the whole genotype matrix into a numpy array.
- ``slice_variants_1k`` — read 1000 evenly-spaced variants × all samples.
- ``slice_samples_10`` — read all variants × 10 samples.
- ``random_10`` — read 10 randomly-chosen (variant, sample) values.
"""

from .base import Operation


def _runner_argv(op_name: str) -> tuple[str, ...]:
    return (
        "${runner}",
        "--bfile", "${prefix}",
        "--op", op_name,
        "--out", "${out}",
    )


OPERATIONS: tuple[Operation, ...] = (
    Operation(
        "bedreader_full_scan",
        "bedreader",
        "bedreader",
        "full_scan",
        _runner_argv("full_scan"),
    ),
    Operation(
        "bedreader_slice_variants_1k",
        "bedreader",
        "bedreader",
        "slice_variants_1k",
        _runner_argv("slice_variants_1k"),
    ),
    Operation(
        "bedreader_slice_samples_10",
        "bedreader",
        "bedreader",
        "slice_samples_10",
        _runner_argv("slice_samples_10"),
    ),
    Operation(
        "bedreader_random_10",
        "bedreader",
        "bedreader",
        "random_10",
        _runner_argv("random_10"),
    ),
)
