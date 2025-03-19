{% extends "base.jinja" %}

{% block statement %}
WITH source AS (
    SELECT
        *
    FROM
        {{ catalog }}.{{ schema }}.{{ table }}
    {%+ if filter %}WHERE {{ filter }}{% endif +%}
)
, records AS (
    SELECT COUNT(1)     AS table_records
    FROM source
)
SELECT
    (SELECT table_records FROM records) AS table_records
    {%+ if unique -%}
        {%- for col in unique -%}
    , ((SELECT COUNT( DISTINCT {{ col }} ) FROM source) = (SELECT table_records FROM records)) AS unique_{{ col }}
        {%- endfor -%}
    {%- endif +%}
    {%+ if notnull -%}
        {%- for col in unique -%}
    , (SELECT COUNT(1) FROM source WHERE {{ col }} IS NULL) = 0 AS notnull_{{ col }}
        {%- endfor -%}
    {%- endif +%}
    {%+ if contain -%}
        {%- for col in contain -%}
    , (SELECT COUNT(1) FROM source WHERE {{ col[0] }} NOT IN {{ col[1] }}) = 0 AS contain_{{ col[0] }}
        {%- endfor -%}
    {%- endif +%}
    {%+ if contain -%}
        {%- for col in validate -%}
    , ((SELECT COUNT(1) FROM source WHERE {{ col[0] }} {{ col[1] }})  = (SELECT table_records FROM records)) AS validate_{{ col[0] }}
        {%- endfor -%}
    {%- endif +%}
{% endblock statement %}
