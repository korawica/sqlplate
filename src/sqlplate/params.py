from dataclasses import dataclass


@dataclass
class DefaultParams:
    database: str
    schema: str
    table: str


@dataclass
class DatabricksParams:
    catalog: str
    schema: str
    table: str
