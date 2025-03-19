{% extends "base.jinja" %}

{% block statement %}
WITH source AS (
    SELECT
        *
    FROM {{ catalog }}.{{ schema }}.{{ table }}
    {%+ if filter %}WHERE {{ filter }}{% endif +%}
)
SELECT
    *
    {%+ if row_count %}, (SELECT COUNT(1) FROM source) AS table_records{% endif +%}
    {%+ if unique -%}
        {%- for col in unique -%}
    , (SELECT COUNT {{ col }} FROM (SELECT DISTINCT {{ col}} FROM source)) AS unique_{{ col }}
        {%- endfor -%}
    {%- endif +%}
FROM source
{% endblock statement %}
