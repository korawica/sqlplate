{% include "utils/etl_vars.jinja" %}
MERGE INTO {{ catalog }}.{{ schema }}.{{ table }} AS target
USING (
    WITH change_query AS (
        SELECT
            src.*,
            CASE WHEN tgt.es_id IS NULL THEN 99
                WHEN hash({_p_col_without_pk_src_str}) <> hash({_p_col_without_pk_tgt_str}) THEN 1
                ELSE 0 END                                        AS data_change
        FROM ( {query} )                                          AS src
        LEFT JOIN {{ catalog }}.{{ schema }}.{{ table }}          AS tgt
            ON {' AND '.join(_p_pk_cols_pairs_sub_query)}
    )
    SELECT * FROM change_query
) AS source
    ON {' AND '.join(_p_pk_cols_pairs)}
WHEN MATCHED AND data_change = 1
THEN UPDATE
    SET {', '.join(_p_col_update)}
    ,   target.delete_f         = 0
    ,   target.prcs_nm          = '{p_process_name}'
    ,   target.prcs_ld_id       = {p_process_load_id}
    ,   target.asat_dt          = {p_asat_dt}
    ,   target.updt_prcs_nm     = '{p_process_name}'
    ,   target.updt_prcs_ld_id  = {p_process_load_id}
    ,   target.updt_asat_dt     = to_timestamp('{p_asat_dt}', 'yyyyMMdd')
WHEN MATCHED AND data_change = 0 AND target.delete_f = 1
THEN UPDATE
    SET target.delete_f         = 0
    ,   target.updt_prcs_nm     = '{p_process_name}'
    ,   target.updt_prcs_ld_id  = {p_process_load_id}
    ,   target.updt_asat_dt     = to_timestamp('{p_asat_dt}', 'yyyyMMdd')
WHEN NOT MATCHED AND data_change = 99
THEN INSERT
    ( {', '.join(i.name for i in rs_col_all)} )
    VALUES (
        {', '.join('source.' + i.name for i in rs_col_real)},
        0, '{p_process_name}', {p_process_load_id}, {p_asat_dt},
        '{p_process_name}', {p_process_load_id}, to_timestamp('{p_asat_dt}', 'yyyyMMdd')
    )
WHEN NOT MATCHED BY SOURCE AND target.delete_f = 0
THEN UPDATE
    SET target.delete_f         = 1
    ,   target.updt_prcs_nm     = '{p_process_name}'
    ,   target.updt_prcs_ld_id  = {p_process_load_id}
    ,   target.updt_asat_dt     = to_timestamp('{p_asat_dt}', 'yyyyMMdd')
