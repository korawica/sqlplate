{% include "utils/etl_vars.jinja" %}
{{ raise_undefined('pk') if pk is undefined }}
{% import "databricks/macros/scd2.jinja" as scd2 %}
{% if pk is iterable and pk is not string and pk is not mapping %}
    {%- set pk_list = pk -%}
{% else %}
    {%- set pk_list = [pk] -%}
{% endif %}
{% if source is defined %}
    {%- set source_query = source|trim -%}
{% elif query is defined %}
    {%- set source_query = '( {} )'.format(query) -%}
{% else %}
    {{ raise_undefined('source|query') }}
{% endif %}
{%- set all_columns = columns + pk_list + scd2_columns -%}
{%- set data_columns = columns + pk_list -%}
MERGE INTO {{ catalog }}.{{ schema }}.{{ table }} AS target
USING (
    WITH change_query AS (
        SELECT
            src.*,
            CASE WHEN tgt.{{ pk_list | first }} IS NULL THEN 99
                WHEN hash({{ columns | map_fmt('src.{0}') | join(', ') }}) <> hash({{ columns | map_fmt('tgt.{0}') | join(', ') }}) THEN 1
                ELSE 0 END AS data_change
        FROM {{ source_query }} AS src
        LEFT JOIN {{ catalog }}.{{ schema }}.{p_table_name} AS tgt
            ON tgt.end_dt = '9999-12-31'
            AND {{ columns | map_fmt("tgt.{0} = src.{0}") | join('\n\t\t\tAND ') }}
    )
    SELECT {{ pk_list | map_fmt('{0} AS merge_{0}') | join(', ') }}, * FROM change_query WHERE data_change == 1
    UNION ALL
    SELECT {{ pk_list | map_fmt('null AS merge_{0}') | join(', ') }}, * FROM change_query WHERE data_change = (1, 99)
) AS source
    ON {{ pk_list | map_fmt('target.{0} = source.merge_{0}') | join('\n\tAND ') }}
WHEN MATCHED AND source.data_change = 1
THEN UPDATE
    SET {{ columns | map_fmt("target.{0}\t\t\t= source.{0}") | join('\n\t,\t') }}
    {{ scd2.sys_update_match(load_src, load_id, load_date) }}
WHEN NOT MATCHED AND source.data_change IN (1, 99)
THEN INSERT
    (
        {{ all_columns | join(', ') }}
    )
VALUES (
    {{ data_columns | map_fmt('source.{0}') | join('\n\t\t,') }}
    ,   to_timestamp('{{ load_date | dt_fmt('%Y%m%d') }}', 'yyyyMMdd')
    ,   to_timestamp('9999-12-31', 'yyyy-MM-dd')
    ,   0
    ,   '{{ load_src }}'
    ,   {{ load_id }}
    ,   {{ load_date | dt_fmt('%Y%m%d') }}
    ,   '{{ load_src }}'
    ,   {{ load_id }}
    ,   to_timestamp('{{ load_date | dt_fmt('%Y%m%d') }}', 'yyyyMMdd')
)
