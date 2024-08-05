{% macro drop_schema(schema_name) %}
{% set sql %}
DROP SCHEMA IF EXISTS {{ schema_name }} CASCADE;
{% endset %}
 
{{ run_query(sql) }}
{% endmacro %}