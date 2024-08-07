
{% macro mirror_raw(source_name, table_name, exclude_columns=[]) %}
    {%- set source_ref = source(source_name, table_name) -%}

    {%- set columns = dbt_utils.get_filtered_columns_in_relation(from=source_ref, except=exclude_columns) -%}

    WITH source AS (
        SELECT
        {%- for column in columns %}
            {{ column }}{{ "," if not loop.last }}
        {%- endfor %}
        FROM {{ source_ref}}
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
