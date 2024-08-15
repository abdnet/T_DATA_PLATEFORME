{{ config(
    materialized = 'incremental',
    unique_key = 'ITEM_ID',
    query_tag = 'dbt_special'
) }}


WITH source AS (
        SELECT
            {{get_columns_by_relation(ref("raw_items"))}}
        FROM {{ ref('raw_items') }}
    ),

step__valid_row AS(
        SELECT
            ID,
            ITEM_ID,
            ORDER_ID,
            PRODUCT_ID,
            SUPPLIER_ID,
            QUANTITY,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM source
        WHERE  {{ not_null_proportion(['ITEM_ID', 'ORDER_ID', 'PRODUCT_ID','SUPPLIER_ID'], 0.8) }}
    ),

step__row_duplicated AS(
   
           {{ dbt_utils.deduplicate(
                relation=ref("raw_items"),
                partition_by='ITEM_ID',
                order_by="UPLOADED_AT desc",
            )
         }}
),
step__avoid_space AS(
        SELECT
            {{avoid_spaces(ref("raw_items"),['QUANTITY','UPLOADED_AT','UPDATED_AT'])}},
            QUANTITY,
            UPLOADED_AT,
            UPDATED_AT
        FROM step__row_duplicated
),

step__imputed AS (
        SELECT
            ID,
            ITEM_ID,
            LOWER(ORDER_ID) AS ORDER_ID,
            LOWER(PRODUCT_ID) AS PRODUCT_ID,
            LOWER(SUPPLIER_ID) AS SUPPLIER_ID,
            COALESCE(QUANTITY, '0') AS QUANTITY,
            COALESCE(UPLOADED_AT, '1900-01-01') AS UPLOADED_AT,
            COALESCE(UPDATED_AT, '1900-01-01') AS UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__avoid_space
),
step__converted AS (
        SELECT
            ID::VARCHAR(250) AS ID,
            ITEM_ID::VARCHAR(250) AS ITEM_ID,
            ORDER_ID::VARCHAR(250) AS ORDER_ID,
            PRODUCT_ID::VARCHAR(250) AS PRODUCT_ID,
            SUPPLIER_ID::VARCHAR(250) AS SUPPLIER_ID,
            QUANTITY::NUMBER(5,0)  AS QUANTITY,
            UPLOADED_AT::TIMESTAMP_NTZ AS UPLOADED_AT,
            UPDATED_AT::TIMESTAMP_NTZ AS UPDATED_AT, 
            SOURCE::VARCHAR(25) AS SOURCE,
            EVENT_TYPE::VARCHAR(25) AS EVENT_TYPE
        FROM step__imputed
),
step__renamed AS (
        SELECT
            ID,
            ITEM_ID,
            ORDER_ID,
            PRODUCT_ID,
            SUPPLIER_ID,
            QUANTITY AS ITEM_QUANTITY,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__converted
),

final AS(
    SELECT
            ID,
            ITEM_ID,
            ORDER_ID,
            PRODUCT_ID,
            SUPPLIER_ID,
            ITEM_QUANTITY,
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
