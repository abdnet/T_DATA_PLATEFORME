{{ config(
    materialized = 'incremental',
    unique_key = 'ORDER_ID',
    query_tag = 'dbt_special'
) }}

WITH source AS (
        SELECT
            {{get_columns_by_relation(ref("raw_orders"))}}
        FROM {{ ref('raw_orders') }}
    ),

step__valid_row AS(
        SELECT
            ID,
	        ORDER_ID,
	        CUSTOMER_ID,
	        STORE_ID,
	        SALESCHANNELID,
	        DATE_ORDER,
	        DATE_DELIVERY,
	        DATE_RETURN,
	        TOTAL_PRICE,
	        UPLOADED_AT,
            UPDATED_AT,
	        SOURCE,
	        EVENT_TYPE
        FROM source
        WHERE  {{ not_null_proportion(['CUSTOMER_ID', 'ORDER_ID', 'STORE_ID','SALESCHANNELID','DATE_ORDER' ], 0.8) }}
    ),

step__row_duplicated AS(
           {{ dbt_utils.deduplicate(
                relation=ref("raw_orders"),
                partition_by='ORDER_ID',
                order_by="UPLOADED_AT DESC",
            )
           }}
),

step__avoid_space AS(
        SELECT
            {{avoid_spaces(ref("raw_orders"),['DATE_ORDER','DATE_DELIVERY','DATE_RETURN','TOTAL_PRICE','UPLOADED_AT', 'UPDATED_AT'])}},
            DATE_ORDER,
	        DATE_DELIVERY,
	        DATE_RETURN,
	        TOTAL_PRICE,
	        UPLOADED_AT,
            UPDATED_AT,
        FROM step__row_duplicated
),

step__standardized AS(
        SELECT
            LOWER(ID) AS ID,
            LOWER(CUSTOMER_ID) AS CUSTOMER_ID,
            LOWER(ORDER_ID) AS ORDER_ID,
            LOWER(STORE_ID) AS STORE_ID,
            LOWER(SALESCHANNELID) AS SALESCHANNELID,
            TO_DATE(TO_CHAR(DATE_ORDER)) AS DATE_ORDER,
            TO_DATE(TO_CHAR(DATE_DELIVERY)) AS DATE_DELIVERY,
            TO_DATE(TO_CHAR(DATE_RETURN)) AS DATE_RETURN,
            TOTAL_PRICE,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__avoid_space
),

step__imputed AS (
        SELECT
            ID,
            ORDER_ID,
            CUSTOMER_ID,
            STORE_ID,
            SALESCHANNELID,
            COALESCE(DATE_ORDER, '1900-01-01') AS DATE_ORDER,
            COALESCE(DATE_DELIVERY, '1900-01-01') AS DATE_DELIVERY,
            DATE_RETURN,
            COALESCE(TOTAL_PRICE, 0) AS TOTAL_PRICE,
            COALESCE(UPLOADED_AT, '1900-01-01') AS UPLOADED_AT,
            COALESCE(UPDATED_AT, '1900-01-01') AS UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__standardized
),
step__converted AS (
        SELECT
            ID::VARCHAR(250) AS ID,
            CUSTOMER_ID::VARCHAR(250) AS CUSTOMER_ID,
            ORDER_ID::VARCHAR(250) AS ORDER_ID,
            STORE_ID::VARCHAR(250) AS STORE_ID,
            SALESCHANNELID::VARCHAR(250) AS SALESCHANNELID,
            DATE_ORDER::DATE AS DATE_ORDER,
            DATE_DELIVERY::DATE AS DATE_DELIVERY,
            DATE_RETURN::DATE AS DATE_RETURN,
            TOTAL_PRICE::NUMBER(38,0) AS TOTAL_PRICE,
            UPLOADED_AT::TIMESTAMP_NTZ AS UPLOADED_AT,
            UPDATED_AT::TIMESTAMP_NTZ AS UPDATED_AT,
            SOURCE::VARCHAR(25) AS SOURCE,
            EVENT_TYPE::VARCHAR(25) AS EVENT_TYPE
        FROM step__imputed
),

step__renamed AS (
        SELECT
            ID,
            CUSTOMER_ID,
            ORDER_ID,
            STORE_ID,
            SALESCHANNELID AS SALES_CHANNEL_ID,
            DATE_ORDER AS ORDER_DATE,
            DATE_DELIVERY AS DELIVERY_DATE,
            DATE_RETURN AS RETURN_DATE,
            TOTAL_PRICE,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__converted
),

final AS(
    SELECT
            ID,
            CUSTOMER_ID,
            ORDER_ID,
            STORE_ID,
            SALES_CHANNEL_ID,
            ORDER_DATE,
            DELIVERY_DATE,
            RETURN_DATE,
            TOTAL_PRICE,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE,
            TIMESTAMPADD('hour', 9, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) AS DBT_UPDATED_DATE
    FROM step__renamed
)

SELECT * FROM final

{% if is_incremental() %}
where UPLOADED_AT >= (SELECT COALESCE(MAX(DBT_UPDATED_DATE),'1900-01-01') FROM {{ this }})   
OR UPDATED_AT >= (SELECT COALESCE(MAX(DBT_UPDATED_DATE),'1900-01-01') FROM {{ this }})    
{% endif %}

