{% extends "base.jinja" %}
{% include "utils/etl_vars.jinja" %}
{{ raise_undefined('pk') if pk is undefined }}
{% import "databricks/macros/scd1.jinja" as scd1 %}
{% from "databricks/macros/utils.jinja" import hash %}
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
{%- set all_columns = columns + pk_list + scd1_soft_delete_columns -%}
{%- set data_columns = columns + pk_list -%}

{% block revert_statement %}
DELETE FROM {{ catalog }}.{{ schema }}.{{ table }}
WHERE
    load_date >= {{ load_date | dt_fmt('%Y%m%d') }},
    AND load_src = '{{ load_src }}'
;
UPDATE {{ catalog }}.{{ schema }}.{{ table }}
    SET delete_f        = 0
    ,   updt_load_src   = '{{ load_src }}'
    ,   updt_load_id    = {{ load_id }}
    ,   updt_load_date  = to_timestamp('{{ load_date | dt_fmt('%Y%m%d') }}', 'yyyyMMdd')
WHERE
    delete_f            = 1
    AND updt_load_src   = '{{ load_src }}'
    AND updt_load_date  >= to_timestamp('{{ load_date | dt_fmt('%Y%m%d') }}', 'yyyyMMdd')
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
                ELSE 0 END                                        AS data_change
        FROM {{ source_query }} AS src
        LEFT JOIN {{ catalog }}.{{ schema }}.{{ table }}          AS tgt
            ON  {{ columns | map_fmt("tgt.{0} = src.{0}") | join('\n\t\t\tAND ') }}
    )
    SELECT * FROM change_query
) AS source
    ON  {{ pk_list | map_fmt('target.{0} = source.{0}') | join('\n\tAND ') }}
WHEN MATCHED AND data_change = 1
THEN UPDATE
    SET {{ columns | map_fmt("target.{0}\t\t\t= source.{0}") | join('\n\t,\t') }}
    ,   {{ scd1.sys_update_match(load_src, load_id, load_date) }}
WHEN MATCHED AND data_change = 0 AND target.delete_f = 1
THEN UPDATE
    SET {{ scd1.sys_update_match(load_src, load_id, load_date) }}
WHEN NOT MATCHED AND data_change = 99
THEN INSERT
    (
        {{ all_columns | join(', ') }}
    )
    VALUES (
        {{ data_columns | map_fmt('source.{0}') | join(',\n\t\t') }},
        0,
        '{{ load_src }}',
        {{ load_id }},
        {{ load_date | dt_fmt('%Y%m%d') }},
        '{{ load_src }}',
        {{ load_id }},
        to_timestamp('{{ load_date | dt_fmt('%Y%m%d') }}', 'yyyyMMdd')
    )
WHEN NOT MATCHED BY SOURCE AND target.delete_f = 0
THEN UPDATE
    SET {{ scd1.sys_update_match(load_src, load_id, load_date, 1) }}
;
{% endblock statement %}
