{{ raise_undefined('columns') if columns is undefined }}
MERGE INTO {{ catalog }}.{{ schema }}.{{ table }} AS target
USING (
    WITH change_query AS (
        SELECT
            src.*,
        CASE WHEN tgt.{% if pk is iterable and pk is not string and pk is not mapping %}{{ pk | first }}{% else %}{{ pk }}{% endif %} IS NULL THEN 99
             WHEN hash({{ columns | map_fmt('src.{0}') | join(', ') }}) <> hash({{ columns | map_fmt('tgt.{0}') | join(', ') }}) THEN 1
             ELSE 0 END AS data_change
        FROM {% if source is defined %}{{ source | trim }}{% elif query is defined %}{{ '( {} )'.format(query) }}{% else %}{{ raise_undefined('source|query') }}{% endif %} AS src
        LEFT JOIN {{ catalog }}.{{ schema }}.{{ table }} AS tgt
            ON {{ columns | map_fmt("tgt.{0} = src.{0}") | join(' AND ') }}
    )
    SELECT * EXCEPT( data_change ) FROM change_query WHERE data_change IN (99, 1)
) AS source
    ON {% if pk is iterable and pk is not string and pk is not mapping %}{{ pk | map_fmt('target.{0} = source.{0}') | join(' AND ') }}{% else %}{{ 'target.{0} = source.{0}'.format(pk) }}{% endif %}
WHEN MATCHED THEN UPDATE
    SET {', '.join(_p_col_update)}
    ,   target.updt_prcs_nm     = '{{ load_src }}'
    ,   target.updt_prcs_ld_id  = {{ load_id }}
    ,   target.updt_asat_dt     = to_timestamp('{p_asat_dt}', 'yyyyMMdd')
WHEN NOT MATCHED THEN INSERT
    (
        {', '.join(i.name for i in rs_col_all)}
    )
    VALUES (
        {', '.join('source.' + i.name for i in rs_col_real)},
        '{{ load_src }}',
        {{ load_id }},
        {p_asat_dt},
        '{{ load_src }}',
        {{ load_id }},
        to_timestamp('{p_asat_dt}', 'yyyyMMdd')
    )
