"""Shared dataclass for the operation matrix.

Kept separate from ``__init__.py`` so per-tool modules can import
``Operation`` without inducing a circular import.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Operation:
    id: str
    tool: str
    category: str
    label: str
    argv: tuple[str, ...]
    aux: tuple[str, ...] = ()
    expensive: bool = False
    timeout_s: int = 600
