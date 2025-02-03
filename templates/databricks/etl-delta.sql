MERGE INTO {p_table_fullname} AS target
USING (
    WITH change_query AS (
        SELECT
            src.*,
            CASE WHEN tgt.es_id IS NULL THEN 99
                WHEN hash({_p_col_without_pk_src_str}) <> hash({_p_col_without_pk_tgt_str}) THEN 1
                ELSE 0 END AS data_change
        FROM ( {query} ) AS src
        LEFT JOIN {p_table_fullname} AS tgt
            ON {' AND '.join(_p_pk_cols_pairs_sub_query)}
    )
    SELECT * EXCEPT( data_change ) FROM change_query WHERE data_change IN (99, 1)
) AS source
    ON {' AND '.join(_p_pk_cols_pairs)}
WHEN MATCHED THEN UPDATE
    SET {', '.join(_p_col_update)}
    ,   target.updt_prcs_nm     = '{p_process_name}'
    ,   target.updt_prcs_ld_id  = {p_process_load_id}
    ,   target.updt_asat_dt     = to_timestamp('{p_asat_dt}', 'yyyyMMdd')
WHEN NOT MATCHED THEN INSERT
    ( {', '.join(i.name for i in rs_col_all)} )
    VALUES (
        {', '.join('source.' + i.name for i in rs_col_real)},
        '{p_process_name}', {p_process_load_id}, {p_asat_dt},
        '{p_process_name}', {p_process_load_id}, to_timestamp('{p_asat_dt}', 'yyyyMMdd')
    )
