SELECT {{ ', '.join(columns) if columns else '*' }}
FROM {{ "{}.".format(catalog) if catalog }}{{ schema }}.{{ table }}
{{ "LIMIT {}".format(limit) if limit }}
