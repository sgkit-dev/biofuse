"""Temporary monkeypatch surfacing two BgenEncoder string-padding
options on ``vcztools.ViewBgenOptions``.

``vcztools.BgenEncoder`` accepts ``total_string_length`` and
``pad_byte`` but ``vcztools.ViewBgenOptions`` does not yet expose
them on the ``view-bgen`` CLI surface. This module subclasses the
original frozen dataclass to add the two fields, layers two click
options on top of the existing ``.decorator``, and extends
``.from_click_kwargs`` to read them.

Importing this module reassigns ``vcztools.ViewBgenOptions`` to the
subclass. ``biofuse/__init__.py`` triggers that side effect so the
patch is in place before any biofuse module resolves the decorator.

Delete this file (and its import in ``biofuse/__init__.py`` and the
``TestViewBgenOptionsShim`` class in ``tests/test_formats.py``) once
vcztools lands the two fields upstream.
"""

import dataclasses

import click
import vcztools

_ORIGINAL = vcztools.ViewBgenOptions


class _PadByteParam(click.ParamType):
    name = "byte"

    def convert(self, value, param, ctx):
        if isinstance(value, bytes):
            return value
        if len(value) != 1 or not value.isascii():
            self.fail("must be a single ASCII character", param, ctx)
        return value.encode("ascii")


@dataclasses.dataclass(frozen=True)
class _ViewBgenOptionsWithStringPadding(_ORIGINAL):
    total_string_length: int | None = None
    pad_byte: bytes | None = None

    @staticmethod
    def decorator(f):
        f = click.option(
            "--pad-byte",
            type=_PadByteParam(),
            default=None,
            help=(
                "Byte that fills slack in each variant's BGEN string "
                "budget. Defaults to '.' inside the encoder."
            ),
        )(f)
        f = click.option(
            "--total-string-length",
            type=click.IntRange(min=1),
            default=None,
            help=(
                "Total byte budget for each variant's BGEN string "
                "fields (varid + rsid + chrom + allele1 + allele2). "
                "Defaults to 64 inside the encoder."
            ),
        )(f)
        f = _ORIGINAL.decorator(f)
        return f

    @classmethod
    def from_click_kwargs(cls, kwargs):
        total_string_length = kwargs.pop("total_string_length", None)
        pad_byte = kwargs.pop("pad_byte", None)
        base = _ORIGINAL.from_click_kwargs(kwargs)
        base_kwargs = {
            f.name: getattr(base, f.name) for f in dataclasses.fields(_ORIGINAL)
        }
        return cls(
            **base_kwargs,
            total_string_length=total_string_length,
            pad_byte=pad_byte,
        )


vcztools.ViewBgenOptions = _ViewBgenOptionsWithStringPadding
