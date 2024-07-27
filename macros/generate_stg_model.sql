{% macro generate_stg_model(source_name, table_name, exclude_columns=[]) %}
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

