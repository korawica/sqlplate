import pytest
from jinja2.exceptions import UndefinedError

from src.sqlplate import SQL


def test_sql_select(template_path):
    select_sql: SQL = (
        SQL.system('databricks', path=template_path)
            .template('select')
            .option('schema', 'schema-name')
            .option('table', 'table-name')
    )
    statement: str = select_sql.load()
    assert statement == (
        "SELECT *\nFROM schema-name.table-name"
    )

    statement: str = (
        select_sql
        .option('catalog', 'catalog-name')
        .load()
    )
    assert statement == (
        "SELECT *\nFROM catalog-name.schema-name.table-name"
    )

    statement: str = (
        select_sql
        .option('limit', 100)
        .load()
    )
    assert statement == (
        "SELECT *\nFROM catalog-name.schema-name.table-name\nLIMIT 100"
    )

    statement: str = (
        select_sql
        .option('columns', ['col01', 'col02'])
        .load()
    )
    assert statement == (
        "SELECT col01, col02\nFROM catalog-name.schema-name.table-name\n"
        "LIMIT 100"
    )


def test_sql_delta(template_path):
    select_sql: SQL = (
        SQL.system('databricks', path=template_path)
        .template('etl.delta')
        .option('catalog', 'catalog-name')
        .option('schema', 'schema-name')
        .option('table', 'table-name')
        .option('pk', 'pk_col')
    )

    with pytest.raises(UndefinedError):
        select_sql.load()

    statement: str = (
        select_sql
        .option('columns', ['col01', 'col02'])
        .option('query', 'SELECT * FROM catalog-name.schema-name.source-name')
        .load()
    )
    print(statement)

    statement: str = (
        select_sql
        .option('pk', ['pk_col01', 'pk_col02'])
        .option('source', 'catalog-name.schema-name.source-name')
        .load()
    )
    print(statement)
