"""Shared utilities: subprocess helpers, version probing, result types."""

import dataclasses
import logging
import pathlib
import shutil
import subprocess
import time

import vcztools
from vcztools.plink import write_plink

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class CheckResult:
    """One discrete check inside a runner."""

    name: str
    passed: bool
    duration_s: float
    detail: str = ""


@dataclasses.dataclass
class RunnerResult:
    """Aggregated result of one runner (posix, fio, ...)."""

    runner: str
    passed: bool
    duration_s: float
    checks: list[CheckResult] = dataclasses.field(default_factory=list)
    skipped: bool = False
    skip_reason: str = ""
    summary: str = ""

    @property
    def num_passed(self) -> int:
        return sum(1 for c in self.checks if c.passed)

    @property
    def num_failed(self) -> int:
        return sum(1 for c in self.checks if not c.passed)


def have_tool(name: str) -> bool:
    return shutil.which(name) is not None


def tool_version(name: str, args: list[str] | None = None) -> str:
    """Best-effort version probe for an external CLI tool."""
    if not have_tool(name):
        return "<not installed>"
    args = args or ["--version"]
    try:
        result = subprocess.run(
            [name, *args],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"<probe failed: {exc}>"
    out = (result.stdout or result.stderr or "").strip()
    first_line = out.splitlines()[0] if out else "<empty>"
    return first_line


class Timer:
    """Context manager for timing a block."""

    def __enter__(self) -> "Timer":
        self.start = time.monotonic()
        self.duration_s = 0.0
        return self

    def __exit__(self, *exc) -> None:
        self.duration_s = time.monotonic() - self.start


def materialise_plink_oracle(
    vcz_path: pathlib.Path,
    dest_dir: pathlib.Path,
    basename: str,
) -> pathlib.Path:
    """Write the plink fileset for ``vcz_path`` into ``dest_dir``.

    Files written: ``{basename}.bed``, ``.bim``, ``.fam``. Returns the
    path to the .bed. The fileset acts as a host-fs
    oracle: every byte biofuse returns from the FUSE-mounted file
    should match the corresponding byte here. Idempotent — if
    ``{basename}.bed`` already exists, this is a no-op.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    bed_path = dest_dir / f"{basename}.bed"
    if bed_path.exists():
        return bed_path
    logger.info("materialising plink oracle at %s", dest_dir / basename)
    reader = vcztools.ViewPlinkOptions().make_reader(str(vcz_path))
    write_plink(reader, dest_dir / basename)
    return bed_path
