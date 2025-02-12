{% extends "base.jinja" %}
{% include "utils/etl_vars.jinja" with context %}
{{ raise_undefined('pk') if pk is undefined }}
{% import "databricks/macros/delta.jinja" as delta %}
{% from "databricks/macros/utils.jinja" import hash, prepare_source %}

{% if pk is iterable and pk is not string and pk is not mapping %}
    {%- set pk_list = pk -%}
{% else %}
    {%- set pk_list = [pk] -%}
{% endif %}

{%- set all_columns = columns + pk_list + etl_columns -%}
{%- set data_columns = columns + pk_list -%}

{% block revert_statement %}
DELETE FROM {{ catalog }}.{{ schema }}.{{ table }}
WHERE
    load_src        = '{{ load_src }}'
    AND load_date   = {{ load_date | dt_fmt('%Y%m%d') }}
;
{% endblock revert_statement %}

{% block statement %}
MERGE INTO {{ catalog }}.{{ schema }}.{{ table }} AS target
USING (
    WITH change_query AS (
        SELECT
            src.*,
        CASE WHEN tgt.{{ pk_list | first }} IS NULL THEN 99
             WHEN {{ hash(columns, mode='normal') }} THEN 1
             ELSE 0 END AS data_change
        FROM {{ prepare_source(source, query) }} AS src
        LEFT JOIN {{ catalog }}.{{ schema }}.{{ table }} AS tgt
            ON  {{ columns | map_fmt("tgt.{0} = src.{0}") | join('\n\t\t\tAND ') }}
    )
    SELECT * EXCEPT( data_change ) FROM change_query WHERE data_change IN (99, 1)
) AS source
    ON  {{ pk_list | map_fmt('target.{0} = source.{0}') | join('\n\tAND ') }}
WHEN MATCHED THEN UPDATE
    SET {{ columns | map_fmt("target.{0}\t\t\t= source.{0}") | join('\n\t,\t') }}
    {{ delta.sys_update_match(load_src, load_id, load_date) }}
WHEN NOT MATCHED THEN INSERT
    (
        {{ all_columns | join(', ') }}
    )
    VALUES (
        {{ data_columns | map_fmt('source.{0}') | join(',\n\t\t') }},
        '{{ load_src }}',
        {{ load_id }},
        {{ load_date | dt_fmt('%Y%m%d') }},
        '{{ load_src }}',
        {{ load_id }},
        to_timestamp('{{ load_date | dt_fmt('%Y%m%d') }}', 'yyyyMMdd')
    )
;
{% endblock statement %}
