{% extends "base.jinja" %}
{% include "utils/etl_vars.jinja" with context %}
{% from "databricks/macros/utils.jinja" import prepare_source %}

{%- set all_columns = columns + etl_columns -%}

{% block revert_statement %}
DELETE FROM {{ catalog }}.{{ schema }}.{{ table }}
WHERE load_src = '{{ load_src }}'
;
{% endblock revert_statement %}

{% block statement %}
INSERT INTO {{ catalog }}.{{ schema }}.{{ table }}
    ( {{ all_columns | join(', ') }} )
SELECT
    {{ columns | join('\n\t,') }}
    ,   '{{ load_src }}'                                                AS load_src
    ,   {{ load_id }}                                                   AS load_id
    ,   {{ load_date | dt_fmt('%Y%m%d') }}                              AS load_date
    ,   '{{ load_src }}'                                                AS updt_load_src
    ,   {{ load_id }}                                                   AS updt_load_id
    ,   to_timestamp('{{ load_date | dt_fmt('%Y%m%d') }}', 'yyyyMMdd')  AS updt_load_date
FROM {{ prepare_source(source, query) }} AS sub_query
;
{% endblock statement %}
