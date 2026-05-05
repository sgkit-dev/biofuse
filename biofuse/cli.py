"""Command-line interface for biofuse."""

import logging
import pathlib
import shutil
import signal
import sys
import tempfile
from functools import wraps

import click
import trio

from biofuse import access_log, fuse_adapter, plink_client, plink_ops

logger = logging.getLogger(__name__)


def _setup_logging(verbosity: int) -> None:
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(verbosity, len(levels) - 1)]
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def handle_exception(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (ValueError, FileNotFoundError, NotADirectoryError) as e:
            raise click.ClickException(str(e)) from e

    return wrapper


def _default_basename(vcz_url: str) -> str:
    """Strip every suffix off ``vcz_url`` to get a plink fileset stem."""
    name = pathlib.Path(vcz_url).name
    while True:
        stem = pathlib.Path(name).stem
        if stem == name:
            return stem
        name = stem


verbose = click.option(
    "-v",
    "--verbose",
    count=True,
    help="Increase logging verbosity (-v info, -vv debug).",
)
backend_storage = click.option(
    "--backend-storage",
    type=click.Choice(["fsspec", "obstore", "icechunk"]),
    default=None,
    help="Backend storage to use for remote VCZ URLs.",
)
access_log_opt = click.option(
    "--access-log",
    "access_log_path",
    type=click.Path(dir_okay=False, path_type=str),
    default=None,
    help="Write per-read access trace as JSONL to PATH.",
)
basename_opt = click.option(
    "--basename",
    type=str,
    default=None,
    help="Basename for the plink fileset (defaults to the VCZ stem).",
)


@click.group()
@click.version_option()
def biofuse_main():
    """biofuse: read-only FUSE filesystem views over VCF Zarr data."""


@biofuse_main.command(name="mount-plink")
@click.argument("vcz_url", type=str)
@click.argument(
    "mount_dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=str),
)
@basename_opt
@backend_storage
@access_log_opt
@verbose
@handle_exception
def mount_plink(
    vcz_url, mount_dir, basename, backend_storage, access_log_path, verbose
):
    """Mount a PLINK 1.9 view of VCZ_URL at MOUNT_DIR.

    Spawns a plink-server subprocess that owns the ``VczReader`` and
    serves ``.bed`` reads over an ``AF_UNIX`` socket. ``.bim`` and
    ``.fam`` are precomputed once at mount time and held in the FUSE
    process's memory; only ``.bed`` reads cross the wire.

    The mount runs in the foreground until interrupted with Ctrl-C.
    """
    _setup_logging(verbose)

    mount_dir_path = pathlib.Path(mount_dir)
    if not mount_dir_path.is_dir():
        raise click.ClickException(f"mount directory does not exist: {mount_dir}")

    log_path = pathlib.Path(access_log_path) if access_log_path is not None else None
    resolved_basename = basename if basename is not None else _default_basename(vcz_url)

    sock_dir = pathlib.Path(tempfile.mkdtemp(prefix="biofuse-"))
    sock_path = sock_dir / "plink.sock"
    try:
        trio.run(
            _amount,
            vcz_url,
            str(mount_dir_path),
            resolved_basename,
            backend_storage,
            log_path,
            sock_path,
        )
    finally:
        shutil.rmtree(sock_dir, ignore_errors=True)


async def _amount(
    vcz_url: str,
    mount_dir: str,
    basename: str,
    backend_storage: str | None,
    log_path: pathlib.Path | None,
    sock_path: pathlib.Path,
) -> None:
    async with await plink_client.PlinkClient.start(
        vcz_url, sock_path, backend_storage=backend_storage
    ) as client:
        with access_log.AccessLogger(log_path) as access_logger:
            ops = plink_ops.PlinkOps(client, basename, access_logger=access_logger)
            async with fuse_adapter.mount(ops, mount_dir):
                click.echo(f"mounted at {mount_dir}", err=True)
                await _wait_for_signal()
                click.echo("unmounting", err=True)


async def _wait_for_signal() -> None:
    """Return on first SIGINT or SIGTERM."""
    with trio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async for _ in signals:
            return


if __name__ == "__main__":
    sys.exit(biofuse_main())
