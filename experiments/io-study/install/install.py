"""Idempotent installer for the IO-study tool zoo.

For each ``ToolSpec`` in ``registry.REGISTRY``, downloads the
distribution artefact (caching under ``_tools/<name>/.cache/``),
extracts the binary into ``_tools/<name>/<exe_name>``, marks it
executable, runs a smoke command, and records the absolute path
in ``_tools/manifest.json`` keyed by ``ToolSpec.name``.

Re-running is a no-op for tools whose manifest entry still exists and
whose smoke command still succeeds. Re-run with ``--force`` to refetch.
"""

import argparse
import fnmatch
import gzip
import hashlib
import json
import logging
import pathlib
import shutil
import subprocess
import sys
import tarfile
import urllib.request
import zipfile

HERE = pathlib.Path(__file__).resolve().parent
STUDY_DIR = HERE.parent
TOOLS_DIR = STUDY_DIR / "_tools"
MANIFEST = TOOLS_DIR / "manifest.json"

sys.path.insert(0, str(HERE))
import registry  # noqa: E402

logger = logging.getLogger(__name__)


def _load_manifest() -> dict[str, str]:
    if not MANIFEST.exists():
        return {}
    return json.loads(MANIFEST.read_text())


def _save_manifest(manifest: dict[str, str]) -> None:
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def _sha256_file(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _download(url: str, dest: pathlib.Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    logger.info("downloading %s", url)
    with urllib.request.urlopen(url) as r, tmp.open("wb") as out:
        shutil.copyfileobj(r, out, length=1 << 20)
    tmp.replace(dest)


def _find_member_in_tar(tar: tarfile.TarFile, pattern: str) -> tarfile.TarInfo:
    matches = [
        m for m in tar.getmembers() if m.isfile() and fnmatch.fnmatch(m.name, pattern)
    ]
    if len(matches) == 0:
        raise RuntimeError(f"no archive member matching {pattern!r}")
    if len(matches) > 1:
        # Prefer the shortest path (typically the right top-level binary,
        # not e.g. a same-named file under example/ or doc/).
        matches.sort(key=lambda m: len(m.name))
    return matches[0]


def _find_member_in_zip(zf: zipfile.ZipFile, pattern: str) -> str:
    matches = [
        n for n in zf.namelist() if not n.endswith("/") and fnmatch.fnmatch(n, pattern)
    ]
    if len(matches) == 0:
        raise RuntimeError(f"no archive member matching {pattern!r}")
    if len(matches) > 1:
        matches.sort(key=len)
    return matches[0]


def _extract_to_exe(
    archive_path: pathlib.Path,
    spec: registry.ToolSpec,
    out_path: pathlib.Path,
) -> None:
    """Place the desired binary at ``out_path`` from ``archive_path``."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if spec.archive == "raw":
        shutil.copyfile(archive_path, out_path)
    elif spec.archive == "gz":
        with gzip.open(archive_path, "rb") as src, out_path.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=1 << 20)
    elif spec.archive in ("tar.gz", "tar.bz2"):
        if spec.archive_member is None:
            raise ValueError(f"{spec.name}: tar archive needs archive_member")
        with tarfile.open(archive_path, "r:*") as tar:
            member = _find_member_in_tar(tar, spec.archive_member)
            extracted = tar.extractfile(member)
            if extracted is None:
                raise RuntimeError(f"{spec.name}: cannot extract {member.name}")
            with out_path.open("wb") as dst:
                shutil.copyfileobj(extracted, dst, length=1 << 20)
    elif spec.archive == "zip":
        if spec.archive_member is None:
            raise ValueError(f"{spec.name}: zip archive needs archive_member")
        with zipfile.ZipFile(archive_path) as zf:
            member = _find_member_in_zip(zf, spec.archive_member)
            with zf.open(member) as src, out_path.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=1 << 20)
    else:
        raise ValueError(f"{spec.name}: unknown archive type {spec.archive!r}")
    out_path.chmod(0o755)


def _extract_extra_member(
    archive_path: pathlib.Path,
    spec: registry.ToolSpec,
    pattern: str,
    rel_dest: str,
) -> None:
    out_path = (TOOLS_DIR / spec.name / rel_dest).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if spec.archive in ("tar.gz", "tar.bz2"):
        with tarfile.open(archive_path, "r:*") as tar:
            member = _find_member_in_tar(tar, pattern)
            extracted = tar.extractfile(member)
            if extracted is None:
                raise RuntimeError(f"{spec.name}: cannot extract {member.name}")
            with out_path.open("wb") as dst:
                shutil.copyfileobj(extracted, dst, length=1 << 20)
    elif spec.archive == "zip":
        with zipfile.ZipFile(archive_path) as zf:
            member = _find_member_in_zip(zf, pattern)
            with zf.open(member) as src, out_path.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=1 << 20)
    else:
        raise ValueError(
            f"{spec.name}: extra_members not supported for archive {spec.archive!r}"
        )


def _smoke_test(exe: pathlib.Path, argv: tuple[str, ...]) -> tuple[bool, str]:
    """Run the binary's smoke command. Return (ok, brief stderr/exit message).

    Many of these tools exit non-zero on ``--help`` (it's idiomatic for
    them to print usage and exit 1). We treat the binary as live if the
    invocation produced output on stderr or stdout; only OS-level errors
    (ENOEXEC, missing libc) are treated as failure.
    """
    try:
        proc = subprocess.run(
            [str(exe), *argv],
            capture_output=True,
            timeout=30,
            check=False,
        )
    except OSError as exc:
        return False, f"OSError: {exc}"
    if proc.stdout or proc.stderr:
        return True, ""
    return False, f"silent exit {proc.returncode}"


def install_one(
    spec: registry.ToolSpec,
    *,
    force: bool,
    manifest: dict[str, str],
) -> tuple[str, str]:
    """Install ``spec``. Returns (status, message); status is one of
    ``installed``, ``skipped``, ``failed``."""
    out_path = TOOLS_DIR / spec.name / spec.exe_name
    cache = TOOLS_DIR / spec.name / ".cache"
    archive_basename = pathlib.Path(spec.url).name
    archive_path = cache / archive_basename

    if not force and out_path.exists() and manifest.get(spec.name) == str(out_path):
        ok, msg = _smoke_test(out_path, spec.smoke_argv)
        if ok:
            return "skipped", str(out_path)

    if force or not archive_path.exists():
        try:
            _download(spec.url, archive_path)
        except Exception as exc:
            return "failed", f"download error: {exc}"

    observed_sha = _sha256_file(archive_path)
    if spec.sha256 is not None and observed_sha != spec.sha256:
        return "failed", f"sha256 mismatch: expected {spec.sha256}, got {observed_sha}"

    try:
        _extract_to_exe(archive_path, spec, out_path)
        for pattern, rel_dest in spec.extra_members:
            _extract_extra_member(archive_path, spec, pattern, rel_dest)
    except Exception as exc:
        return "failed", f"extract error: {exc}"

    ok, msg = _smoke_test(out_path, spec.smoke_argv)
    if not ok:
        return "failed", f"smoke test failed: {msg}"

    manifest[spec.name] = str(out_path)
    if spec.sha256 is None:
        manifest[f"{spec.name}__observed_sha256"] = observed_sha
    return "installed", f"{out_path}  sha256={observed_sha}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-download and re-extract even if already installed",
    )
    parser.add_argument(
        "--only",
        nargs="*",
        default=None,
        help="install only the named tools",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    TOOLS_DIR.mkdir(parents=True, exist_ok=True)
    manifest = _load_manifest()

    specs = list(registry.REGISTRY)
    if args.only:
        wanted = set(args.only)
        specs = [s for s in specs if s.name in wanted]

    n_failed = 0
    for spec in specs:
        status, msg = install_one(spec, force=args.force, manifest=manifest)
        print(f"{status:9s} {spec.name:10s} {msg}")
        if status == "failed":
            n_failed += 1
        # Save manifest after every tool so partial runs are not wasted.
        _save_manifest(manifest)

    if n_failed > 0:
        logger.warning("%d tool(s) failed to install", n_failed)
        sys.exit(1)


if __name__ == "__main__":
    main()
