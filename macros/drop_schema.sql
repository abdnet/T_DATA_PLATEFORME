{% macro drop_schema(schema_name) %}
{% set sql %}
DROP SCHEMA IF EXISTS {{ schema_name }} CASCADE;
{% endset %}

{{ run_query(sql) }}
{% endmacro %}

{% macro drop_table(table_name) %}
{% set sql %}
DROP TABLE IF EXISTS {{ ref('table_name') }};
{% endset %}

{{ run_query(sql) }}
{% endmacro %}
