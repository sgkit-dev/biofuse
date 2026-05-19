"""Command-line interface for biofuse."""

import logging
import pathlib
import signal
import sys
from functools import wraps

import click
import trio
import vcztools

from biofuse import access_log, encoder_host, encoder_ops, formats, fuse_adapter

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

    The FUSE handler owns the ``VczReader`` and one fresh
    :class:`vcztools.BedEncoder` per open ``.bed`` fh; ``encoder.read``
    runs on worker threads so the pyfuse3 main loop stays responsive.
    ``.bim`` and ``.fam`` are precomputed once at mount time and held
    in memory; only ``.bed`` reads invoke the encoder. ``--no-bim`` and
    ``--no-fam`` suppress the corresponding sidecar from the mount.

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

    The FUSE handler owns the ``VczReader`` and one fresh
    :class:`vcztools.BgenEncoder` per open ``.bgen`` fh; ``encoder.read``
    runs on worker threads so the pyfuse3 main loop stays responsive.
    ``.sample`` and ``.bgen.bgi`` are precomputed once at mount time
    and held in memory; only ``.bgen`` reads invoke the encoder.
    ``--no-sample-file`` and ``--no-bgi`` suppress the corresponding
    sidecar; ``--no-header-samples`` drops the sample identifiers from
    the ``.bgen`` header block. ``--unphased`` ignores the input's
    ``call_genotype_phased`` field and encodes every variant with the
    BGEN phased flag clear.

    The bcftools-view-style filter / backend / log options are inherited
    from ``vcztools view-bgen``; see ``vcztools view-bgen --help`` for the
    full reference. The encoder always uses zlib level 0 (stored,
    fixed-size blocks) so byte-range random access into the mounted
    ``.bgen`` is O(1).

    BGEN tuning parameters:

    \b
    * ``--total-string-length`` (default 64) is the combined byte budget
      per variant for the five BGEN string slots (varid + rsid + chrom +
      allele1 + allele2). Every variant block reserves exactly this many
      bytes for the string section, which is what makes the mounted
      ``.bgen`` byte-offset-addressable. The defaults are tuned for
      biobank biallelic SNP arrays where rsids, single-base alleles, and
      short contig names fit comfortably. Raise it when the input has
      long indel alleles, long contig names (e.g.
      ``chrUn_KI270742v1``), or non-rsid variant IDs that would
      otherwise overflow the budget — if any variant's actual string
      content sums past ``total_string_length - 1`` the encoder raises
      a ``ValueError`` at read time.
    * ``--pad-byte`` (default ``.``) fills the slack inside each
      variant's padding string after the leading ``.``. The default
      makes the padding indistinguishable from the leading delimiter;
      override it (e.g. ``--pad-byte X``) only when you want the
      boundary visible in a hex dump for debugging.

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

    trio.run(
        _amount,
        spec,
        vcz_url,
        str(mount_dir_path),
        resolved_basename,
        opts,
        log_path,
    )


async def _amount(
    spec: formats.FormatSpec,
    vcz_url: str,
    mount_dir: str,
    basename: str,
    opts,
    log_path: pathlib.Path | None,
) -> None:
    async with await encoder_host.EncoderHost.start(
        vcz_url,
        spec,
        opts=opts,
    ) as host:
        with access_log.AccessLogger(log_path) as access_logger:
            ops = encoder_ops.EncoderOps(
                host, basename, spec, access_logger=access_logger
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
