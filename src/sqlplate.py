from pathlib import Path
from typing import Any

from jinja2 import Template

from .utils import get_env
from .exceptions import TemplateNotSet


class SQL:
    """A SQL object for render any SQL template that prepare by Jinja package."""

    def __init__(self, name: str, path: Path):
        self.name: str = name

        if not path.exists():
            raise FileNotFoundError(f"Path {path} does not exists.")

        self.path: Path = path
        self._template: Template | None = None
        self._option: dict[str, Any] = {}

    @classmethod
    def system(cls, name: str, path: Path | None = None) -> 'SQL':
        """Construction this class from a system value name.

        Args:
            name (str): A system name of the SQL template.
            path (Path | None): A template path.
        """
        if path is None:
            path: Path = Path('./templates')
        return cls(name=name, path=path)

    def template(self, name: str) -> 'SQL':
        """Create template object attribute on this instance."""
        self._template: Template = (
            get_env(self.path).get_template(f'{self.name}/{name}.sql')
        )
        return self

    def option(self, key: str, value: Any) -> 'SQL':
        self._option[key] = value
        return self

    def options(self, values: dict[str, Any]) -> 'SQL':
        self._option = self._option | values
        return self

    def load(self) -> str:
        if self._template is None:
            raise TemplateNotSet(
                "Template object does not create before load, you should use "
                "`.template(name=?)`."
            )
        return self._template.render(**self._option)
