"""Filesystem view backed by a real on-disk directory.

Used in phase 1 to wrap the temporary directory produced by
``vcztools.plink.write_plink``: biofuse's only job is to expose those files
through FUSE, so a passthrough is sufficient.

Design notes:

- Only the top-level files of the backing directory are exposed; nested
  subdirectories and non-regular entries are ignored.
- Each ``open`` call allocates a fresh ``os.open`` file descriptor; reads
  use ``os.pread`` so handles do not carry a position. This keeps
  concurrent reads on the same handle correct without per-handle locking.
- The directory listing is cached at construction time. The view is
  read-only and the backing directory is not expected to change while
  mounted, so this avoids per-readdir disk traffic.
"""

import logging
import os
import pathlib
import stat
import threading

from biofuse import access_log, view

logger = logging.getLogger(__name__)


class PassthroughDirectoryView:
    """A read-only FilesystemView over a flat on-disk directory."""

    def __init__(
        self,
        directory: pathlib.Path,
        *,
        access_logger: access_log.AccessLogger | None = None,
    ) -> None:
        self._directory = pathlib.Path(directory)
        self._access_logger = access_logger
        self._entries: dict[str, view.FileEntry] = {}
        self._handles: dict[int, tuple[str, int]] = {}
        self._next_fh = 1
        self._lock = threading.Lock()
        self._scan()

    def _scan(self) -> None:
        if not self._directory.is_dir():
            raise NotADirectoryError(self._directory)
        for child in sorted(self._directory.iterdir()):
            if not child.is_file():
                continue
            st = child.stat()
            self._entries[child.name] = view.FileEntry(
                name=child.name,
                size=st.st_size,
                mtime_ns=st.st_mtime_ns,
                mode=stat.S_IFREG | 0o444,
            )

    def list(self) -> list[view.FileEntry]:
        return list(self._entries.values())

    def stat(self, name: str) -> view.FileEntry:
        try:
            return self._entries[name]
        except KeyError as exc:
            raise FileNotFoundError(name) from exc

    def open(self, name: str) -> int:
        if name not in self._entries:
            raise FileNotFoundError(name)
        path = self._directory / name
        fd = os.open(path, os.O_RDONLY)
        with self._lock:
            fh = self._next_fh
            self._next_fh += 1
            self._handles[fh] = (name, fd)
        return fh

    def read(self, fh: int, offset: int, size: int) -> bytes:
        if size <= 0:
            return b""
        with self._lock:
            try:
                name, fd = self._handles[fh]
            except KeyError as exc:
                raise OSError(f"read on unknown or released handle {fh}") from exc
        data = os.pread(fd, size, offset)
        if self._access_logger is not None:
            self._access_logger.record(name, offset, len(data))
        return data

    def release(self, fh: int) -> None:
        with self._lock:
            entry = self._handles.pop(fh, None)
        if entry is None:
            return
        _, fd = entry
        os.close(fd)

    def close(self) -> None:
        """Release any handles still open. Safe to call multiple times."""
        with self._lock:
            handles = list(self._handles.items())
            self._handles.clear()
        for _, (_, fd) in handles:
            try:
                os.close(fd)
            except OSError:
                pass

    def __enter__(self) -> "PassthroughDirectoryView":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
