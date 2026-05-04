"""Command-line interface for biofuse."""

import logging
import pathlib
import signal
import sys
import threading
from functools import wraps

import click

from biofuse import access_log, bed_client, fuse_adapter, plink_ops

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

    Serves ``.bed`` on demand via a worker subprocess running
    ``vcztools.BedEncoder``; ``.bim`` and ``.fam`` are computed once at
    mount time and held in the worker's memory.

    The mount runs in the foreground until interrupted with Ctrl-C.
    """
    _setup_logging(verbose)

    mount_dir_path = pathlib.Path(mount_dir)
    if not mount_dir_path.is_dir():
        raise click.ClickException(f"mount directory does not exist: {mount_dir}")

    log_path = pathlib.Path(access_log_path) if access_log_path is not None else None
    resolved_basename = basename if basename is not None else _default_basename(vcz_url)

    client = bed_client.BedEncoderClient(
        vcz_url, resolved_basename, backend_storage=backend_storage
    )
    try:
        with access_log.AccessLogger(log_path) as access_logger:
            ops = plink_ops.PlinkOps(client, access_logger=access_logger)
            with fuse_adapter.Mount(ops, str(mount_dir_path)):
                click.echo(f"mounted at {mount_dir_path}", err=True)
                _wait_for_signal()
                click.echo("unmounting", err=True)
    finally:
        client.close()


def _wait_for_signal() -> None:
    """Block the calling thread until SIGINT or SIGTERM is received."""
    stop = threading.Event()

    def handler(signum, frame):
        stop.set()

    previous_int = signal.signal(signal.SIGINT, handler)
    previous_term = signal.signal(signal.SIGTERM, handler)
    try:
        stop.wait()
    finally:
        signal.signal(signal.SIGINT, previous_int)
        signal.signal(signal.SIGTERM, previous_term)


if __name__ == "__main__":
    sys.exit(biofuse_main())
