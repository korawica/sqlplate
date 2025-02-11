# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from jinja2 import Environment, PackageLoader
from jinja2.exceptions import UndefinedError


def map_fmt(value: list[str], fmt: str) -> list[str]:
    """Prepare string value with formatting string.

    Example:
        >>> map_fmt(['col01', 'col02'], fmt='src.{0} = tgt.{0}')
        ['src.col01 = tgt.col01', 'src.col02 = tgt.col02']
    """
    return [fmt.format(i) for i in value]


def raise_undefined(value: str) -> None:
    """Raise with UndefinedError for a needed variable on the Jinja template."""
    if len(value.split('|')) > 1:
        value: str = "' or '".join(value.split('|'))
    raise UndefinedError(f"The '{value}' is undefined")


def dt_fmt(value: datetime, fmt: str) -> str:
    """Format a datetime object to string value."""
    return value.strftime(fmt)


def get_env(
    path: Path,
    trim_blocks: bool = True,
    lstrip_blocks: bool = True,
) -> Environment:
    """Get jinja environment object for the SQL template files.

    Args:
        path (Path): A package path.
        trim_blocks (bool):
        lstrip_blocks (bool):
    """
    env = Environment(
        loader=PackageLoader(
            package_name='templates',
            package_path=str(path),
        ),
        trim_blocks=trim_blocks,
        lstrip_blocks=lstrip_blocks
    )
    env.filters['map_fmt'] = map_fmt
    env.filters['dt_fmt'] = dt_fmt
    env.globals['raise_undefined'] = raise_undefined
    return env
