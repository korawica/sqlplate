# SQL Template

A SQLPlate (SQL template) generator that is a generator object for SQL template
statement.

**The layer of SQL template files will be:**

```text
📂templates/
   ├─ 📂databricks/
   │     ├─ 📜 etl.delta.sql
   │     ├─ 📜 etl.scd2.sql
   │     ╰─ 📜 select.sql
   ├─ 📂synapse-dedicate/
   │     ╰─ 📜 etl.delta.sql
   ╰─ 📂utils/
         ╰─ 📜 etl_vars.jinja
```

## :package: Installation

```shell
pip install -U sqlplate
```

## :fork_and_knife: Usage

```python
from datetime import datetime
from sqlplate import SQLPlate

statement: str = (
    SQLPlate.system('databricks')
    .template('etl.delta')
    .option('catalog', 'catalog-name')
    .option('schema', 'schema-name')
    .option('table', 'table-name')
    .option('pk', 'pk_col')
    .option('columns', ['col01', 'col02'])
    .option('query', 'SELECT * FROM catalog-name.schema-name.source-name')
    .option('load_src', 'SOURCE_FOO')
    .option('load_id', 1)
    .option('load_date', datetime(2025, 2, 1, 10))
    .load()
)
print(statement.strip().strip('\n'))
```

```sql
MERGE INTO catalog-name.schema-name.table-name AS target
USING (
    WITH change_query AS (
        SELECT
            src.*,
        CASE WHEN tgt.pk_col IS NULL THEN 99
             WHEN hash(src.col01, src.col02) <> hash(tgt.col01, tgt.col02) THEN 1
             ELSE 0 END AS data_change
        FROM ( SELECT * FROM catalog-name.schema-name.source-name ) AS src
        LEFT JOIN catalog-name.schema-name.table-name AS tgt
            ON  tgt.col01 = src.col01
AND tgt.col02 = src.col02
    )
    SELECT * EXCEPT( data_change ) FROM change_query WHERE data_change IN (99, 1)
) AS source
    ON  target.pk_col = source.pk_col
WHEN MATCHED THEN UPDATE
    SET target.col01            = source.col01
    ,   target.col02            = source.col02
    ,   target.updt_prcs_nm     = 'SOURCE_FOO'
    ,   target.updt_prcs_ld_id  = 1
    ,   target.updt_asat_dt     = to_timestamp('20250201', 'yyyyMMdd')
WHEN NOT MATCHED THEN INSERT
    (
        col01, col02, pk_col, start_dt, end_dt, delete_f, prcs_nm, prcs_ld_id, asat_dt, updt_prcs_nm, updt_prcs_ld_id, updt_asat_dt
    )
    VALUES (
        source.col01,
        source.col02,
        source.pk_col,
        'SOURCE_FOO',
        1,
        20250201,
        'SOURCE_FOO',
        1,
        to_timestamp('20250201', 'yyyyMMdd')
    )
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
