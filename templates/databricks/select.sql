SELECT *
FROM {{ catalog }}.{{ schema }}.{{ table }}
{{% if limit %}}
LIMIT {{ limit }}
{{% else %}}
{{% endif %}}
