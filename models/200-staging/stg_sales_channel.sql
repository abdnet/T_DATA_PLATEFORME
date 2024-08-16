{{ config(
    materialized = 'incremental',
    unique_key = 'SALES_CHANNEL_ID',
    query_tag = 'dbt_special'
) }}


WITH source AS (
        SELECT
            {{get_columns_by_relation(ref("raw_sales_channel"))}}
        FROM {{ ref('raw_sales_channel') }}
    ),

step__valid_row AS(
        SELECT
            ID,
            SALESCHANNELID,
            NAME,
            DESCRIPTION,
            IS_ACTIVE,
            CREATED_AT,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM source
        WHERE  {{ not_null_proportion(['SALESCHANNELID', 'NAME'], 0.8) }}
    ),

step__row_duplicated AS(
   
           {{ dbt_utils.deduplicate(
                relation=ref("raw_sales_channel"),
                partition_by='SALESCHANNELID',
                order_by="UPLOADED_AT desc",
            )
         }}
),

step__avoid_space AS(
        SELECT
            {{avoid_spaces(ref("raw_sales_channel"),['CREATED_AT','UPLOADED_AT','UPDATED_AT'])}},
            CREATED_AT,
            UPLOADED_AT,
            UPDATED_AT
        FROM step__row_duplicated
),

step__imputed AS (
        SELECT
            ID,
            SALESCHANNELID,
            COALESCE(NAME, 'UNKNOWN') AS NAME,
            COALESCE(DESCRIPTION, 'UNKNOWN') AS DESCRIPTION,
            IS_ACTIVE,
            COALESCE(CREATED_AT, '1900-01-01') AS CREATED_AT,
            COALESCE(UPLOADED_AT, '1900-01-01') AS UPLOADED_AT,
            COALESCE(UPDATED_AT, '1900-01-01') AS UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__avoid_space
),
step__standardized AS(
        SELECT
            LOWER(ID) AS ID,
            LOWER(SALESCHANNELID) AS SALESCHANNELID,
            INITCAP(NAME) AS NAME,
            DESCRIPTION,
            UPPER(IS_ACTIVE) AS IS_ACTIVE,
            TO_DATE(CREATED_AT) AS CREATED_AT,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__imputed
),

step__converted AS (
        SELECT
            ID::VARCHAR(250) AS ID,
            SALESCHANNELID::VARCHAR(250) AS SALESCHANNELID,
            NAME::VARCHAR(100) AS NAME,
            DESCRIPTION::VARCHAR(100) AS DESCRIPTION,
            IS_ACTIVE::BOOLEAN AS IS_ACTIVE,
            CREATED_AT::DATE AS CREATED_AT,
            UPLOADED_AT::TIMESTAMP_NTZ AS UPLOADED_AT,
            UPDATED_AT::TIMESTAMP_NTZ AS UPDATED_AT, 
            SOURCE::VARCHAR(25) AS SOURCE,
            EVENT_TYPE::VARCHAR(25) AS EVENT_TYPE

        FROM step__standardized
),

step__renamed AS (
        SELECT
            ID,
            SALESCHANNELID AS SALES_CHANNEL_ID,
            NAME,
            DESCRIPTION,
            IS_ACTIVE,
            CREATED_AT,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__converted
),

final AS(
    SELECT
            ID,
            SALES_CHANNEL_ID,
            NAME,
            DESCRIPTION,
            IS_ACTIVE,
            CREATED_AT,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE,
            TIMESTAMPADD('hour', 9, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) AS DBT_UPDATED_DATE
    FROM step__renamed
)

SELECT * FROM final

{% if is_incremental() %}
where UPLOADED_AT >= (select COALESCE(max(DBT_UPDATED_DATE),'1900-01-01') from {{ this }})   
OR UPDATED_AT >= (select COALESCE(max(DBT_UPDATED_DATE),'1900-01-01') from {{ this }})    
{% endif %}
