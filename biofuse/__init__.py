from biofuse import _vcztools_compat  # noqa: F401  — applies temporary monkeypatch

try:
    from biofuse._version import version as __version__
except ImportError:
    __version__ = "unknown"
