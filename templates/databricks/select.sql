{{ raise_undefined('schema') if schema is undefined }}
{{ raise_undefined('table') if table is undefined }}
SELECT {{ ', '.join(columns) if columns else '*' }}
FROM {{ "{}.".format(catalog) if catalog }}{{ schema }}.{{ table }}
{{ "LIMIT {}".format(limit) if limit }}
