"""Command-line interface for biofuse."""

import logging
import pathlib
import signal
import sys
import tempfile
from functools import wraps

import click
import trio
import vcztools

from biofuse import access_log, encoder_client, encoder_ops, formats, fuse_adapter

logger = logging.getLogger(__name__)


def handle_exception(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (ValueError, FileNotFoundError, NotADirectoryError) as e:
            raise click.ClickException(str(e)) from e
        except OSError as e:
            raise click.ClickException(e.strerror or str(e)) from e

    return wrapper


def _default_basename(vcz_url: str) -> str:
    """Strip every suffix off ``vcz_url`` to get a fileset stem."""
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


def _basename_opt(format_label: str) -> click.Option:
    return click.option(
        "--basename",
        type=str,
        default=None,
        help=f"Basename for the {format_label} fileset (defaults to the VCZ stem).",
    )


@click.group()
@click.version_option()
def biofuse_main():
    """biofuse: read-only FUSE filesystem views over VCF Zarr data."""


@biofuse_main.command(name="mount-plink", cls=vcztools.GroupedCommand)
@click.argument("vcz_url", type=str)
@click.argument(
    "mount_dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=str),
)
@_basename_opt("plink")
@vcztools.ViewPlinkOptions.decorator
@access_log_opt
@handle_exception
def mount_plink(vcz_url, mount_dir, basename, access_log_path, **kwargs):
    """Mount a PLINK 1.9 view of VCZ_URL at MOUNT_DIR.

    Spawns a plink-server subprocess that owns the ``VczReader`` and
    serves ``.bed`` reads over an ``AF_UNIX`` socket. ``.bim`` and
    ``.fam`` are precomputed once at mount time and held in the FUSE
    process's memory; only ``.bed`` reads cross the wire. ``--no-bim``
    and ``--no-fam`` suppress the corresponding sidecar from the mount.

    The bcftools-view-style filter / backend / log options are inherited
    from ``vcztools view-plink``; see ``vcztools view-plink --help`` for the
    full reference.

    The mount runs in the foreground until interrupted with Ctrl-C.
    """
    opts = vcztools.ViewPlinkOptions.from_click_kwargs(kwargs)
    _run_mount(formats.PLINK_SPEC, vcz_url, mount_dir, basename, access_log_path, opts)


@biofuse_main.command(name="mount-bgen", cls=vcztools.GroupedCommand)
@click.argument("vcz_url", type=str)
@click.argument(
    "mount_dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=str),
)
@_basename_opt("bgen")
@vcztools.ViewBgenOptions.decorator
@access_log_opt
@handle_exception
def mount_bgen(vcz_url, mount_dir, basename, access_log_path, **kwargs):
    """Mount an Oxford BGEN view of VCZ_URL at MOUNT_DIR.

    Spawns a bgen-server subprocess that owns the ``VczReader`` and
    serves ``.bgen`` reads over an ``AF_UNIX`` socket. ``.sample`` and
    ``.bgen.bgi`` are precomputed once at mount time and held in the
    FUSE process's memory; only ``.bgen`` reads cross the wire.
    ``--no-sample-file`` and ``--no-bgi`` suppress the corresponding
    sidecar; ``--no-header-samples`` drops the sample identifiers from
    the ``.bgen`` header block.

    The bcftools-view-style filter / backend / log options are inherited
    from ``vcztools view-bgen``; see ``vcztools view-bgen --help`` for the
    full reference. The encoder always uses zlib level 0 (stored,
    fixed-size blocks) so byte-range random access into the mounted
    ``.bgen`` is O(1).

    The mount runs in the foreground until interrupted with Ctrl-C.
    """
    opts = vcztools.ViewBgenOptions.from_click_kwargs(kwargs)
    _run_mount(formats.BGEN_SPEC, vcz_url, mount_dir, basename, access_log_path, opts)


def _run_mount(
    spec: formats.FormatSpec,
    vcz_url: str,
    mount_dir: str,
    basename: str | None,
    access_log_path: str | None,
    opts,
) -> None:
    opts.log.apply()

    mount_dir_path = pathlib.Path(mount_dir)
    if not mount_dir_path.is_dir():
        raise click.ClickException(f"mount directory does not exist: {mount_dir}")

    log_path = pathlib.Path(access_log_path) if access_log_path is not None else None
    resolved_basename = basename if basename is not None else _default_basename(vcz_url)

    with tempfile.TemporaryDirectory(prefix="biofuse-") as sock_dir:
        sock_path = pathlib.Path(sock_dir) / f"{spec.name}.sock"
        trio.run(
            _amount,
            spec,
            vcz_url,
            str(mount_dir_path),
            resolved_basename,
            opts,
            log_path,
            sock_path,
        )


async def _amount(
    spec: formats.FormatSpec,
    vcz_url: str,
    mount_dir: str,
    basename: str,
    opts,
    log_path: pathlib.Path | None,
    sock_path: pathlib.Path,
) -> None:
    async with await encoder_client.EncoderClient.start(
        vcz_url,
        sock_path,
        spec,
        opts=opts,
    ) as client:
        with access_log.AccessLogger(log_path) as access_logger:
            ops = encoder_ops.EncoderOps(
                client, basename, spec, access_logger=access_logger
            )
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
