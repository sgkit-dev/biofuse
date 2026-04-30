"""Access pattern logging.

Records each ``read`` operation observed by the filesystem view as an
``(path, offset, size, monotonic_time)`` row. The in-memory mode keeps rows
in a list for tests and aggregation; the JSONL mode appends a single line
per row to a file for offline analysis (e.g. characterising what a real
PLINK invocation actually reads).
"""

import json
import logging
import pathlib
import threading
import time
from dataclasses import asdict, dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AccessRecord:
    path: str
    offset: int
    size: int
    t_monotonic: float


class AccessLogger:
    """Records read operations.

    If ``out_path`` is None, records are kept in memory only (use ``records``
    to read them). If a path is provided, each record is appended as a JSONL
    line; in-memory recording is skipped to keep memory bounded for long-
    running mounts.

    Thread-safe: a single lock serialises both modes. The locking is coarse
    but the access path is not performance-critical compared with the
    underlying disk read.
    """

    def __init__(self, out_path: pathlib.Path | None = None) -> None:
        self._out_path = pathlib.Path(out_path) if out_path is not None else None
        self._records: list[AccessRecord] = []
        self._lock = threading.Lock()
        self._fh = None
        if self._out_path is not None:
            self._fh = self._out_path.open("a", buffering=1)

    @property
    def records(self) -> list[AccessRecord]:
        with self._lock:
            return list(self._records)

    def record(self, path: str, offset: int, size: int) -> None:
        rec = AccessRecord(
            path=path, offset=offset, size=size, t_monotonic=time.monotonic()
        )
        with self._lock:
            if self._fh is not None:
                self._fh.write(json.dumps(asdict(rec)) + "\n")
            else:
                self._records.append(rec)

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                self._fh.close()
                self._fh = None

    def __enter__(self) -> "AccessLogger":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
