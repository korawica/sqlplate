{% extends "databricks/etl.scd2.sql" %}
{% import "databricks/macros/scd2.jinja" as scd2 +%}
{% block substatement %}
WHEN NOT MATCHED BY SOURCE
    AND target.end_dt   = to_timestamp('9999-12-31', 'yyyy-MM-dd')
    AND target.prcs_nm  = '{p_process_name}'
THEN UPDATE
    SET target.delete_f         = 1
    {{ scd2.sys_update_match(load_src, load_id, load_date) }}
{% endblock substatement %}
