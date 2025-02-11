SELECT *
FROM {{ "{}.".format(catalog) if catalog }}{{ schema }}.{{ table }}
{{ "LIMIT {}".format(limit) if limit }}
