{% extends "base.jinja" %}
{{ raise_undefined('catalog') if catalog is undefined }}
{{ raise_undefined('schema') if schema is undefined }}
{{ raise_undefined('table') if table is undefined }}

{% block statement %}
WITH source AS (
    SELECT
        *
    FROM
        {{ catalog }}.{{ schema }}.{{ table }}
    {%+ if filter %}WHERE {{ filter }}{% endif +%}
)
, records AS (
    SELECT COUNT(1) AS table_records FROM source
)
SELECT
    (SELECT table_records FROM records) AS table_records
    {%+ if unique -%}
        {%- for col in unique -%}
    , ((SELECT COUNT( DISTINCT {{ col }} ) FROM source) = (SELECT table_records FROM records)) AS unique_{{ col }}
        {%- endfor -%}
    {%- endif +%}
    {%+ if notnull -%}
        {%- for col in notnull -%}
    , (SELECT COUNT(1) FROM source WHERE {{ col }} IS NULL) = 0 AS notnull_{{ col }}
        {%- endfor -%}
    {%- endif +%}
    {%- if validates -%}
        {%- for validate in validates -%}
            {%- for col in validate.cols +%}
    , ((SELECT COUNT(1) FROM source WHERE {{ col }} {{ validate.condition }})  = (SELECT table_records FROM records)) AS {{ validate.rule }}_{{ col }}
            {%- endfor -%}
        {% endfor -%}
    {%- endif +%}
{% endblock statement %}
