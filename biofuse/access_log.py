"""Access pattern logging.

Records each ``read`` operation observed by the filesystem view as an
``(path, fh, offset, size, t_start, t_end)`` row. Both timestamps are
captured so post-hoc analysis can detect overlapping reads — when two
records share a ``[t_start, t_end]`` window but carry distinct ``fh``,
the underlying RPC pipeline was actually concurrent.

The in-memory mode keeps rows in a list for tests and aggregation; the
JSONL mode appends a single line per row to a file for offline
analysis (e.g. characterising what a real PLINK invocation actually
reads).
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
    fh: int
    offset: int
    size: int
    t_start: float
    t_end: float
    kind: str = "read"


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

    def record(
        self,
        path: str,
        fh: int,
        offset: int,
        size: int,
        t_start: float,
    ) -> None:
        """Record one read.

        ``t_start`` is the monotonic timestamp the caller captured before
        issuing the read; ``t_end`` is captured inside this call. The
        difference is the read's wall-clock duration; overlap between
        records with distinct ``fh`` indicates concurrent execution.
        """
        self._write(
            AccessRecord(
                path=path,
                fh=fh,
                offset=offset,
                size=size,
                t_start=t_start,
                t_end=time.monotonic(),
            )
        )

    def record_event(
        self,
        kind: str,
        path: str,
        fh: int,
        t_start: float,
        t_end: float | None = None,
    ) -> None:
        """Record a non-read lifecycle event (open / release / limiter_wait /
        aclose). ``offset`` and ``size`` are zero for these events; the
        ``[t_start, t_end]`` interval is what matters."""
        if t_end is None:
            t_end = time.monotonic()
        self._write(
            AccessRecord(
                path=path,
                fh=fh,
                offset=0,
                size=0,
                t_start=t_start,
                t_end=t_end,
                kind=kind,
            )
        )

    def _write(self, rec: AccessRecord) -> None:
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
