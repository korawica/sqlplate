# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

from jinja2 import Template

from .conf import config
from .exceptions import TemplateNotSet
from .utils import get_env, remove_sql_comment


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
        self._template_name: str | None = None
        self._template_type: str | None = None
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
        self._template_name: str = name

        if '.' in name and name.count('.') == 1:
            self._template_type, _ = name.split('.', maxsplit=1)

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

    def load(self, remove_comment: bool = False, **kwargs) -> str:
        """Generate the SQL statement from its template setup.

        Args:
            - remove_comment (bool): Remove comment after the template render.
        """
        if self._template_name is None or self._template is None:
            raise TemplateNotSet(
                "Template object does not create before load, you should use "
                "`.template(name=?)`."
            )
        render: str = (
            self._template.render(
                **(
                    {"_system": self.name, "_template": self._template_name}
                    | config().export(self._template_type)
                    | self._option
                    | kwargs
                )
            )
            .strip()
            .strip('\n')
        )
        if remove_comment:
            return remove_sql_comment(render)
        return render

    def stream(
        self,
        remove_comment: bool = False,
        split_char: str = ';',
        **kwargs
    ) -> Iterator[str]:
        """Return the iterator of sub-statement that split with ';' charactor.

        Args:
            - remove_comment (bool): Remove comment after the template render.
            - split_char (str): A charactor that want to split from the full
                statement. Default is ';'.
        """
        yield from (
            s.strip().strip('\n')
            for s in (
                self.load(remove_comment=remove_comment, **kwargs)
                .split(split_char)
            )
            if s.strip().strip('\n') != ''
        )
