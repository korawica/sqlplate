from datetime import datetime
from textwrap import dedent

import pytest
from jinja2.exceptions import UndefinedError
from src.sqlplate import SQLPlate

from .utils import prepare_statement


def test_sql_select(template_path):
    select_sql: SQLPlate = (
        SQLPlate.system('databricks', path=template_path)
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
    select_sql: SQLPlate = (
        SQLPlate.system('databricks', path=template_path)
        .template('etl.delta')
        .option('catalog', 'catalog-name')
        .option('schema', 'schema-name')
        .option('table', 'table-name')
        .option('pk', 'pk_col')
        .option('load_src', 'SOURCE_FOO')
        .option('load_id', 1)
        .option('load_date', datetime(2025, 2, 1, 10))
    )

    with pytest.raises(UndefinedError):
        select_sql.load()

    statement: str = (
        select_sql
        .option('columns', ['col01', 'col02'])
        .option('query', 'SELECT * FROM catalog-name.schema-name.source-name')
        .load()
    )
    assert prepare_statement(statement) == dedent("""
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
            SET target.col01= source.col01
        ,target.col02= source.col02
            ,   target.updt_load_src    = 'SOURCE_FOO'
            ,   target.updt_load_id     = 1
            ,   target.updt_load_date   = to_timestamp('20250201', 'yyyyMMdd')
        WHEN NOT MATCHED THEN INSERT
            (
                col01, col02, pk_col, load_src, load_id, load_date, updt_load_src, updt_load_id, updt_load_date
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
        """).strip('\n')

    statement: str = (
        select_sql
        .option('pk', ['pk_col01', 'pk_col02'])
        .option('source', 'catalog-name.schema-name.source-name')
        .load()
    )
    assert prepare_statement(statement) == dedent("""
        MERGE INTO catalog-name.schema-name.table-name AS target
        USING (
            WITH change_query AS (
                SELECT
                    src.*,
                CASE WHEN tgt.pk_col01 IS NULL THEN 99
                     WHEN hash(src.col01, src.col02) <> hash(tgt.col01, tgt.col02) THEN 1
                     ELSE 0 END AS data_change
                FROM catalog-name.schema-name.source-name AS src
                LEFT JOIN catalog-name.schema-name.table-name AS tgt
                    ON  tgt.col01 = src.col01
        AND tgt.col02 = src.col02
            )
            SELECT * EXCEPT( data_change ) FROM change_query WHERE data_change IN (99, 1)
        ) AS source
            ON  target.pk_col01 = source.pk_col01
        AND target.pk_col02 = source.pk_col02
        WHEN MATCHED THEN UPDATE
            SET target.col01= source.col01
        ,target.col02= source.col02
            ,   target.updt_load_src    = 'SOURCE_FOO'
            ,   target.updt_load_id     = 1
            ,   target.updt_load_date   = to_timestamp('20250201', 'yyyyMMdd')
        WHEN NOT MATCHED THEN INSERT
            (
                col01, col02, pk_col01, pk_col02, load_src, load_id, load_date, updt_load_src, updt_load_id, updt_load_date
            )
            VALUES (
                source.col01,
        source.col02,
        source.pk_col01,
        source.pk_col02,
                'SOURCE_FOO',
                1,
                20250201,
                'SOURCE_FOO',
                1,
                to_timestamp('20250201', 'yyyyMMdd')
            )
        """).strip('\n')


def test_sql_scd1_soft_delete(template_path):
    select_sql: SQLPlate = (
        SQLPlate.system('databricks', path=template_path)
        .template('etl.scd1-soft-delete')
        .option('catalog', 'catalog-name')
        .option('schema', 'schema-name')
        .option('table', 'table-name')
        .option('pk', 'pk_col')
        .option('load_src', 'SOURCE_FOO')
        .option('load_id', 1)
        .option('load_date', datetime(2025, 2, 1, 10))
    )
    statement: str = (
        select_sql
        .option('columns', ['col01', 'col02'])
        .option('query', 'SELECT * FROM catalog-name.schema-name.source-name')
        .load()
    )
    assert prepare_statement(statement) == dedent("""
        MERGE INTO catalog-name.schema-name.table-name AS target
        USING (
            WITH change_query AS (
                SELECT
                    src.*,
                    CASE WHEN tgt.pk_col IS NULL THEN 99
                        WHEN hash(src.col01, src.col02) <> hash(tgt.col01, tgt.col02) THEN 1
                        ELSE 0 END                                        AS data_change
                FROM ( SELECT * FROM catalog-name.schema-name.source-name ) AS src
                LEFT JOIN catalog-name.schema-name.table-name          AS tgt
                    ON  tgt.col01 = src.col01
        AND tgt.col02 = src.col02
            )
            SELECT * FROM change_query
        ) AS source
            ON  target.pk_col = source.pk_col
        WHEN MATCHED AND data_change = 1
        THEN UPDATE
            SET target.col01= source.col01
        ,target.col02= source.col02
            ,   target.delete_f         = 0
            ,   target.updt_load_src    = 'SOURCE_FOO'
            ,   target.updt_load_id     = 1
            ,   target.updt_load_date   = to_timestamp('20250201', 'yyyyMMdd')
        WHEN MATCHED AND data_change = 0 AND target.delete_f = 1
        THEN UPDATE
            SET target.delete_f         = 0
            ,   target.updt_load_src    = 'SOURCE_FOO'
            ,   target.updt_load_id     = 1
            ,   target.updt_load_date   = to_timestamp('20250201', 'yyyyMMdd')
        WHEN NOT MATCHED AND data_change = 99
        THEN INSERT
            (
                col01, col02, pk_col, delete_f, load_src, load_id, load_date, updt_load_src, updt_load_id, updt_load_date
            )
            VALUES (
                source.col01,
        source.col02,
        source.pk_col,
                0,
                'SOURCE_FOO',
                1,
                20250201,
                'SOURCE_FOO',
                1,
                to_timestamp('20250201', 'yyyyMMdd')
            )
        WHEN NOT MATCHED BY SOURCE AND target.delete_f = 0
        THEN UPDATE
            SET target.delete_f         = 1
            ,   target.updt_load_src    = 'SOURCE_FOO'
            ,   target.updt_load_id     = 1
            ,   target.updt_load_date   = to_timestamp('20250201', 'yyyyMMdd')
        """).strip('\n')


def test_sql_scd2(template_path):
    select_sql: SQLPlate = (
        SQLPlate.system('databricks', path=template_path)
        .template('etl.scd2')
        .option('catalog', 'catalog-name')
        .option('schema', 'schema-name')
        .option('table', 'table-name')
        .option('pk', 'pk_col')
        .option('load_src', 'SOURCE_FOO')
        .option('load_id', 1)
        .option('load_date', datetime(2025, 2, 1, 10))
    )
    statement: str = (
        select_sql
        .option('columns', ['col01', 'col02'])
        .option('query', 'SELECT * FROM catalog-name.schema-name.source-name')
        .load()
    )
    assert prepare_statement(statement) == dedent("""
        MERGE INTO catalog-name.schema-name.table-name AS target
        USING (
            WITH change_query AS (
                SELECT
                    src.*,
                    CASE WHEN tgt.pk_col IS NULL THEN 99
                        WHEN hash(src.col01, src.col02) <> hash(tgt.col01, tgt.col02) THEN 1
                        ELSE 0 END AS data_change
                FROM ( SELECT * FROM catalog-name.schema-name.source-name ) AS src
                LEFT JOIN catalog-name.schema-name.{p_table_name} AS tgt
                    ON tgt.end_dt = '9999-12-31'
                    AND tgt.col01 = src.col01
        AND tgt.col02 = src.col02
            )
            SELECT pk_col AS merge_pk_col, * FROM change_query WHERE data_change == 1
            UNION ALL
            SELECT null AS merge_pk_col, * FROM change_query WHERE data_change = (1, 99)
        ) AS source
            ON target.pk_col = source.merge_pk_col
        WHEN MATCHED AND source.data_change = 1
        THEN UPDATE
            SET target.col01= source.col01
        ,target.col02= source.col02
            ,   target.end_dt           = DATEADD(DAY, -1, to_timestamp('20250201', 'yyyyMMdd'))
            ,   target.updt_load_src    = 'SOURCE_FOO'
            ,   target.updt_load_id     = 1
            ,   target.updt_load_date   = to_timestamp('20250201', 'yyyyMMdd')
        WHEN NOT MATCHED AND source.data_change IN (1, 99)
        THEN INSERT
            (
                col01, col02, pk_col, start_date, end_date, delete_f, load_src, load_id, load_date, updt_load_src, updt_load_id, updt_load_date
            )
        VALUES (
            source.col01
        ,source.col02
        ,source.pk_col
            ,   to_timestamp('20250201', 'yyyyMMdd')
            ,   to_timestamp('9999-12-31', 'yyyy-MM-dd')
            ,   0
            ,   'SOURCE_FOO'
            ,   1
            ,   20250201
            ,   'SOURCE_FOO'
            ,   1
            ,   to_timestamp('20250201', 'yyyyMMdd')
        )
        """).strip('\n')


def test_sql_scd2_delete_src(template_path):
    select_sql: SQLPlate = (
        SQLPlate.system('databricks', path=template_path)
        .template('etl.scd2-delete-src')
        .option('catalog', 'catalog-name')
        .option('schema', 'schema-name')
        .option('table', 'table-name')
        .option('pk', 'pk_col')
        .option('load_src', 'SOURCE_FOO')
        .option('load_id', 1)
        .option('load_date', datetime(2025, 2, 1, 10))
    )
    statement: str = (
        select_sql
        .option('columns', ['col01', 'col02'])
        .option('query', 'SELECT * FROM catalog-name.schema-name.source-name')
        .load()
    )
    assert prepare_statement(statement) == dedent("""
        MERGE INTO catalog-name.schema-name.table-name AS target
        USING (
            WITH change_query AS (
                SELECT
                    src.*,
                    CASE WHEN tgt.pk_col IS NULL THEN 99
                        WHEN hash(src.col01, src.col02) <> hash(tgt.col01, tgt.col02) THEN 1
                        ELSE 0 END AS data_change
                FROM ( SELECT * FROM catalog-name.schema-name.source-name ) AS src
                LEFT JOIN catalog-name.schema-name.{p_table_name} AS tgt
                    ON tgt.end_dt = '9999-12-31'
                    AND tgt.col01 = src.col01
        AND tgt.col02 = src.col02
            )
            SELECT pk_col AS merge_pk_col, * FROM change_query WHERE data_change == 1
            UNION ALL
            SELECT null AS merge_pk_col, * FROM change_query WHERE data_change = (1, 99)
        ) AS source
            ON target.pk_col = source.merge_pk_col
        WHEN MATCHED AND source.data_change = 1
        THEN UPDATE
            SET target.col01= source.col01
        ,target.col02= source.col02
            ,   target.end_dt           = DATEADD(DAY, -1, to_timestamp('20250201', 'yyyyMMdd'))
            ,   target.updt_load_src    = 'SOURCE_FOO'
            ,   target.updt_load_id     = 1
            ,   target.updt_load_date   = to_timestamp('20250201', 'yyyyMMdd')
        WHEN NOT MATCHED AND source.data_change IN (1, 99)
        THEN INSERT
            (
                col01, col02, pk_col, start_date, end_date, delete_f, load_src, load_id, load_date, updt_load_src, updt_load_id, updt_load_date
            )
        VALUES (
            source.col01
        ,source.col02
        ,source.pk_col
            ,   to_timestamp('20250201', 'yyyyMMdd')
            ,   to_timestamp('9999-12-31', 'yyyy-MM-dd')
            ,   0
            ,   'SOURCE_FOO'
            ,   1
            ,   20250201
            ,   'SOURCE_FOO'
            ,   1
            ,   to_timestamp('20250201', 'yyyyMMdd')
        )
        WHEN NOT MATCHED BY SOURCE
            AND target.end_dt   = to_timestamp('9999-12-31', 'yyyy-MM-dd')
            AND target.prcs_nm  = '{p_process_name}'
        THEN UPDATE
            SET target.delete_f         = 1
            ,   target.end_dt           = DATEADD(DAY, -1, to_timestamp('20250201', 'yyyyMMdd'))
            ,   target.updt_load_src    = 'SOURCE_FOO'
            ,   target.updt_load_id     = 1
            ,   target.updt_load_date   = to_timestamp('20250201', 'yyyyMMdd')
        """).strip('\n')
