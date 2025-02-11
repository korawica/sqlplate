# SQL Template

A SQLPlate (SQL template) is store and generate SQL template project.

```text
templates
  ├─ databricks/
  │    ├─ etl-delta.sql
  │    ╰─ etl-scd2.sql
  ╰─ synapse-dedicate/
       ╰─ delta.sql
```

## Usage

```python
from datetime import datetime
from src.sqlplate import SQL

statement: str = (
    SQL.system('databricks')
        .template('etl.scd2')
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

```text
```

## :speech_balloon: Contribute

I do not think this project will go around the world because it has specific propose,
and you can create by your coding without this project dependency for long term
solution. So, on this time, you can open [the GitHub issue on this project :raised_hands:](https://github.com/korawica/sqlplate/issues)
for fix bug or request new feature if you want it.
