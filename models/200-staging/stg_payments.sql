{{ config(
    materialized = 'incremental',
    unique_key = 'PAYMENT_ID',
    query_tag = 'dbt_special'
) }}

WITH source AS (
        SELECT
            {{get_columns_by_relation(ref("raw_payments"))}}
        FROM {{ ref('raw_payments') }}
    ),

step__valid_row AS(
        SELECT
	        ID,
            PAYMENT_ID,
            ORDER_ID,
            AMOUNT,
            PAYMENT_METHOD,
            PAYMENT_STATUS,
            TRANSACTION_ID,
            CREATED_AT,
            PAYMENT_UPDATED_AT,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM source
        WHERE  {{ not_null_proportion(['PAYMENT_ID', 'ORDER_ID','AMOUNT' ], 0.8) }}
    ),

step__row_duplicated AS(
           {{ dbt_utils.deduplicate(
                relation=ref("raw_payments"),
                partition_by='PAYMENT_ID',
                order_by="UPLOADED_AT DESC",
            )
           }}
),

step__avoid_space AS(
        SELECT
            {{avoid_spaces(ref("raw_payments"),['AMOUNT','CREATED_AT','PAYMENT_UPDATED_AT','UPLOADED_AT', 'UPDATED_AT'])}},
            AMOUNT,
	        CREATED_AT,
	        PAYMENT_UPDATED_AT,
	        UPLOADED_AT,
            UPDATED_AT,
        FROM step__row_duplicated
),

step__standardized AS(
        SELECT
            LOWER(ID) AS ID,
            LOWER(PAYMENT_ID) AS PAYMENT_ID,
            LOWER(ORDER_ID) AS ORDER_ID,
            AMOUNT,
            INITCAP(PAYMENT_METHOD) AS PAYMENT_METHOD,
            UPPER(PAYMENT_STATUS) AS PAYMENT_STATUS,
            LOWER(TRANSACTION_ID) AS TRANSACTION_ID,
            TO_DATE(TO_CHAR(CREATED_AT)) AS CREATED_AT,
            TO_DATE(TO_CHAR(PAYMENT_UPDATED_AT)) AS PAYMENT_UPDATED_AT,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__avoid_space
),
step__imputed AS (
        SELECT
            ID,
            PAYMENT_ID,
            ORDER_ID,
            COALESCE(AMOUNT, '0') AS AMOUNT,
            COALESCE(PAYMENT_METHOD, 'UNKNOWN') AS PAYMENT_METHOD,
            COALESCE(PAYMENT_STATUS, 'UNKNOWN') AS PAYMENT_STATUS,
            TRANSACTION_ID,
            COALESCE(CREATED_AT, '1900-01-01') AS CREATED_AT,
            COALESCE(PAYMENT_UPDATED_AT, '1900-01-01') AS PAYMENT_UPDATED_AT,
            COALESCE(UPLOADED_AT, '1900-01-01') AS UPLOADED_AT,
            COALESCE(UPDATED_AT, '1900-01-01') AS UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__standardized
),
step__converted AS (
        SELECT
            ID::VARCHAR(250) AS ID,
            PAYMENT_ID::VARCHAR(250) AS PAYMENT_ID,
            ORDER_ID::VARCHAR(250) AS ORDER_ID,
            AMOUNT::NUMBER(38,0) AS AMOUNT,
            PAYMENT_METHOD::VARCHAR(70) AS PAYMENT_METHOD,
            PAYMENT_STATUS::VARCHAR(50) AS PAYMENT_STATUS,
            TRANSACTION_ID::VARCHAR(250) AS TRANSACTION_ID,
            CREATED_AT::DATE AS CREATED_AT,
            PAYMENT_UPDATED_AT::DATE AS PAYMENT_UPDATED_AT,
            UPLOADED_AT::TIMESTAMP_NTZ AS UPLOADED_AT,
            UPDATED_AT::TIMESTAMP_NTZ AS UPDATED_AT,
            SOURCE::VARCHAR(25) AS SOURCE,
            EVENT_TYPE::VARCHAR(25) AS EVENT_TYPE
        FROM step__imputed
),
step__renamed AS (
        SELECT
            ID,
            PAYMENT_ID,
            ORDER_ID,
            AMOUNT AS PAYMENT_AMOUNT,
            PAYMENT_METHOD,
            PAYMENT_STATUS,
            TRANSACTION_ID,
            CREATED_AT,
            PAYMENT_UPDATED_AT,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__converted
),
final AS(
    SELECT
            ID,
            PAYMENT_ID,
            ORDER_ID,
            PAYMENT_AMOUNT,
            PAYMENT_METHOD,
            PAYMENT_STATUS,
            TRANSACTION_ID,
            CREATED_AT,
            PAYMENT_UPDATED_AT,
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

