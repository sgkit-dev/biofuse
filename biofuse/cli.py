"""Command-line interface for biofuse."""

import logging
import pathlib
import signal
import sys
import tempfile
from functools import wraps

import click
import trio
from vcztools import cli as vcztools_cli

from biofuse import access_log, fuse_adapter, plink_client, plink_ops

logger = logging.getLogger(__name__)


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
@vcztools_cli.view_bed_options
@access_log_opt
@vcztools_cli.log_options
@handle_exception
def mount_plink(vcz_url, mount_dir, basename, access_log_path, **kwargs):
    """Mount a PLINK 1.9 view of VCZ_URL at MOUNT_DIR.

    Spawns a plink-server subprocess that owns the ``VczReader`` and
    serves ``.bed`` reads over an ``AF_UNIX`` socket. ``.bim`` and
    ``.fam`` are precomputed once at mount time and held in the FUSE
    process's memory; only ``.bed`` reads cross the wire.

    The bcftools-view-style filter / backend / log options are inherited
    from ``vcztools view-bed``; see ``vcztools view-bed --help`` for the
    full reference.

    The mount runs in the foreground until interrupted with Ctrl-C.
    """
    log_config = vcztools_cli.LogConfig.pop_from_click_kwargs(kwargs)
    reader_options = vcztools_cli.ViewBedOptions.pop_from_click_kwargs(kwargs)
    assert kwargs == {}, kwargs
    log_config.apply()

    mount_dir_path = pathlib.Path(mount_dir)
    if not mount_dir_path.is_dir():
        raise click.ClickException(f"mount directory does not exist: {mount_dir}")

    log_path = pathlib.Path(access_log_path) if access_log_path is not None else None
    resolved_basename = basename if basename is not None else _default_basename(vcz_url)

    with tempfile.TemporaryDirectory(prefix="biofuse-") as sock_dir:
        sock_path = pathlib.Path(sock_dir) / "plink.sock"
        trio.run(
            _amount,
            vcz_url,
            str(mount_dir_path),
            resolved_basename,
            reader_options,
            log_config,
            log_path,
            sock_path,
        )


async def _amount(
    vcz_url: str,
    mount_dir: str,
    basename: str,
    reader_options: vcztools_cli.ViewBedOptions,
    log_config: vcztools_cli.LogConfig,
    log_path: pathlib.Path | None,
    sock_path: pathlib.Path,
) -> None:
    async with await plink_client.PlinkClient.start(
        vcz_url,
        sock_path,
        reader_options=reader_options,
        log_config=log_config,
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
