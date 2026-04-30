"""Filesystem-shaped interface that the FUSE adapter consumes.

Concrete implementations (e.g. PassthroughDirectoryView) live in their own
modules. The interface is deliberately small: read-only, flat directory,
file handles allocated by ``open`` and reclaimed by ``release``.
"""

import logging
import stat
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileEntry:
    """Metadata for a single file in the mounted view.

    ``mode`` is the POSIX mode bits including file type. For read-only
    regular files this is typically ``stat.S_IFREG | 0o444``.
    """

    name: str
    size: int
    mtime_ns: int
    mode: int = stat.S_IFREG | 0o444


@runtime_checkable
class FilesystemView(Protocol):
    """The minimal read-only filesystem surface the FUSE adapter requires."""

    def list(self) -> list[FileEntry]:
        """Return entries for every file at the mount root."""
        ...

    def stat(self, name: str) -> FileEntry:
        """Return metadata for a single named file. Raises FileNotFoundError."""
        ...

    def open(self, name: str) -> int:
        """Allocate a file handle for ``name`` and return its integer id."""
        ...

    def read(self, fh: int, offset: int, size: int) -> bytes:
        """Return up to ``size`` bytes starting at ``offset`` for handle ``fh``."""
        ...

    def release(self, fh: int) -> None:
        """Release the handle. Subsequent reads on it must fail."""
        ...
