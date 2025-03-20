{{ raise_undefined('table') if table is undefined }}
SELECT {{ ', '.join(columns) if columns else '*' }}
FROM {{ table }}
