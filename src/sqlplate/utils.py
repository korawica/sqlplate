# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, PackageLoader


def get_env(path: Path):
    """Get jinja environment object for the SQL template files.

    Args:
        path (Path): A package path.
    """
    return Environment(
        loader=PackageLoader(
            package_name='templates',
            package_path=str(path),
        ),
    )
