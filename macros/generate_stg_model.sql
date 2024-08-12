
{% macro mirror_raw(source_name, table_name, exclude_columns=[]) %}
    {%- set source_ref = source(source_name, table_name) -%}

    {%- set columns = dbt_utils.get_filtered_columns_in_relation(from=source_ref, except=exclude_columns) -%}

    WITH source AS (
        SELECT
        {%- for column in columns %}
            {{ column }}{{ "," if not loop.last }}
        {%- endfor %}
        FROM {{ source_ref }}
    )

    SELECT
    {%- for column in columns %}
        {{ column }}{{ "," if not loop.last }}
    {%- endfor %}

    FROM source
{% endmacro %}


{% macro get_columns_by_relation(model_ref, exclude_columns=[])%}
    {%- set columns = dbt_utils.get_filtered_columns_in_relation(from=model_ref, except=exclude_columns) -%}

    {%- for column in columns %}
            {{ column }}{{ "," if not loop.last }}
    {%- endfor %}

{% endmacro %}

{% macro avoid_spaces(model_ref, exclude_columns=[]) %}

    {%- set columns = dbt_utils.get_filtered_columns_in_relation(from=model_ref, except=exclude_columns) -%}

    {%- for column in columns %}
              TRIM({{ column }}) AS {{column}}{{ "," if not loop.last }}
    {%- endfor %}

{% endmacro %}

{%- macro not_null_proportion(columns, threshold) -%}
    {%- set total_columns = columns | length %}

    {%- if total_columns == 0 %}
        {% do raise("La liste des colonnes ne doit pas être vide.") %}
    {% endif %}

    {% set threshold_value = total_columns * threshold %}

    {% set threshold_rounded = (threshold_value) | round  %}

    {% set case_statements = [] %}
    {%- for column in columns %}
        {%- set case_statement = "CASE WHEN " ~ column ~ " IS NOT NULL THEN 1 ELSE 0 END" -%}
        {%- do case_statements.append(case_statement) -%}
    {%- endfor -%}

    {%- set case_sum = case_statements | join(' + ') %}
    {%- set sql_condition = "(" ~ case_sum ~ ") >= " ~ threshold_rounded -%}
    {{ sql_condition | trim }}
{%- endmacro -%}

{% macro get_columns(model_ref) %}

    {% set ephemeral_sql %}
        SELECT *
        FROM {{ model_ref }}
    {% endset %}

    {{ log(ephemeral_sql, info=True) }}

    {% set columns_query %}
        WITH cte AS ({{ ephemeral_sql }})
        SELECT * FROM cte LIMIT 0
    {% endset %}
    {{ log(columns_query, info=True) }}

    {% set results = run_query(columns_query) %}
    {% set columns = [] %}

    {% for column in results.columns %}
        {% do columns.append(column.name) %}
    {% endfor %}
    {{ log(columns, info=True) }}

    {{ columns | join(', ') }}
{% endmacro %}

