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
        .option('catalog', 'catalog-name')
        .option('limit', 100)
        .load()
    )
    assert statement == (
        "SELECT *\nFROM catalog-name.schema-name.table-name\nLIMIT 100"
    )
