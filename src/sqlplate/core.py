# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2 import Template

from .utils import get_env
from .exceptions import TemplateNotSet


class SQLPlate:
    """A SQLPlate object for render any SQL template that prepare by Jinja
    template.

        This object cas pass an option with dot pattern like func-programing.
    """

    def __init__(self, name: str, path: Path) -> None:
        self.name: str = name

        if not path.exists():
            raise FileNotFoundError(f"Path {path} does not exists.")

        self.path: Path = path
        self._template: Template | None = None
        self._option: dict[str, Any] = {}

    @classmethod
    def system(cls, name: str, path: Path | None = None) -> 'SQLPlate':
        """Construction this class from a system value name.

        Args:
            name (str): A system name of the SQLPlate template.
            path (Path | None): A template path.
        """
        if path is None:
            path: Path = Path('./templates')
        return cls(name=name, path=path)

    def template(self, name: str) -> 'SQLPlate':
        """Create template object attribute on this instance."""
        self._template: Template = (
            get_env(self.path).get_template(f'{self.name}/{name}.sql')
        )
        return self

    def option(self, key: str, value: Any) -> 'SQLPlate':
        """Pass an option key-value pair before generate template."""
        self._option[key] = value
        return self

    def options(self, values: dict[str, Any]) -> 'SQLPlate':
        """Pass an option mapping with multiple key-value pairs before generate
        template.

        Args:
            values (dict[str, Any]): A mapping of multiple key-value pairs.
        """
        self._option = self._option | values
        return self

    def load(self) -> str:
        """Generate the SQL statement from its template setup."""
        if self._template is None:
            raise TemplateNotSet(
                "Template object does not create before load, you should use "
                "`.template(name=?)`."
            )
        return self._template.render(**self._option).strip().strip('\n')
