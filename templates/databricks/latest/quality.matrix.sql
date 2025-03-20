{% extends "base.jinja" %}
{{ raise_undefined('catalog') if catalog is undefined }}
{{ raise_undefined('schema') if schema is undefined }}
{{ raise_undefined('table') if table is undefined }}
{{ raise_undefined('columns') if columns is undefined }}


{% block statement %}
WITH source AS (
    SELECT
        *
    FROM {{ catalog }}.{{ schema }}.{{ table }}
    {%+ if filter %}WHERE {{ filter }}{% endif +%}
)
, records AS (
    SELECT COUNT(1) AS table_records FROM source
)
{% for col in columns %}
SELECT
    "{{ col }}" AS name
    , table_records AS `count`
    , (SELECT COUNT_IF({{ col }} IS NULL) FROM source) AS null_count
    , (SELECT MEAN({{ col }}) FROM source) AS `mean`
    , (SELECT STD({{ col }}) FROM source) AS std
    , (SELECT MIN({{ col }}) FROM source) AS `min`
    , (SELECT PERCENTILE({{ col }}, 0.25) FROM source) AS `25%`
    , (SELECT PERCENTILE({{ col }}, 0.5) FROM source) AS `50%`
    , (SELECT PERCENTILE({{ col }}, 0.75) FROM source) AS `75%`
    , (SELECT MAX({{ col }}) FROM source) AS `max`
FROM table_records
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
{% endblock statement %}
