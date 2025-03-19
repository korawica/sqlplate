{% extends "base.jinja" %}

{% block statement %}
WITH source AS (
    SELECT
        *
    FROM {{ catalog }}.{{ schema }}.{{ table }}
    {%+ if filter %}WHERE {{ filter }}{% endif +%}
)
SELECT
{% endblock statement %}
