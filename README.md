# SQL Template

A SQLPlate (SQL template) generator that is a generator object for SQL template
statement.

**The layer of SQL template files will be:**

```text
📂templates/
   ├─ 📂databricks/
   │     ├─ 📜 etl-delta.sql
   │     ╰─ 📜 etl-scd2.sql
   ╰─ 📂synapse-dedicate/
         ╰─ 📜 delta.sql
```

## :package: Installation

```shell
pip install -U sqlplate
```

> [!WARNING]
> This package does not exist on the PyPI yet.

## :fork_and_knife: Usage

```python
from datetime import datetime
from src.sqlplate import SQL

statement: str = (
    SQL.system('databricks')
        .template('etl.scd2')
        .option('catalog', 'catalog-name')
        .option('schema', 'schema-name')
        .option('table', 'table-name')
        .option('pk', ['pk_col_name'])
        .option('columns', ['pk_col_name', 'col_01', 'col_02', 'col_03'])
        .option('load_date', f"{datetime.now():%Y-%m-%d %H:%M:%S}")
        .option('load_src', 'SOURCE_SYS_FOO')
        .load()
)
print(statement)
```

```sql
MERGE INTO catalog-name.schema-name.table-name AS target
USING (
    ...
) AS source
    ON target.pk_col_name = source.pk_col_name
WHEN MATCH AND source.data_change = 1
THEN UPDATE
    SET ...
```

## Systems

| System     |    Progress     | Version  | Remark  |
|:-----------|:---------------:|:--------:|---------|
| databricks | :yellow_circle: |          |         |
| sqlite     | :yellow_circle: |          |         |

> [!NOTE]
> - :green_circle:: Complete
> - :yellow_circle:: In progress
> - :red_circle:: Does not develop
> - :purple_circle:: Does not plan to support

## :speech_balloon: Contribute

I do not think this project will go around the world because it has specific propose,
and you can create by your coding without this project dependency for long term
solution. So, on this time, you can open [the GitHub issue on this project :raised_hands:](https://github.com/korawica/sqlplate/issues)
for fix bug or request new feature if you want it.
