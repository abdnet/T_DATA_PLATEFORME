{{ config(
    materialized = 'incremental',
    unique_key = 'SUPPLIER_ID',
    query_tag = 'dbt_special'
) }}


WITH source AS (
        SELECT
            {{get_columns_by_relation(ref("raw_suplliers"))}}
        FROM {{ ref('raw_suplliers') }}
    ),

step__valid_row AS(
        SELECT
            ID,
            SUPPLIER_ID,
            NAME,
            CONTACT_NAME,
            PHONE,
            EMAIL,
            ADRESS,
            COUNTRY,
            REGION,
            CITY,
            POSTAL_CODE,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM source
        WHERE  {{ not_null_proportion(['SUPPLIER_ID', 'NAME', 'EMAIL','ADRESS'], 0.8) }}
    ),

step__row_duplicated AS(
   
           {{ dbt_utils.deduplicate(
                relation=ref("raw_suplliers"),
                partition_by='SUPPLIER_ID',
                order_by="UPLOADED_AT desc",
            )
         }}
),

step__avoid_space AS(
        SELECT
            {{avoid_spaces(ref("raw_suplliers"),['UPLOADED_AT','UPDATED_AT','POSTAL_CODE'])}},
            UPLOADED_AT,
            UPDATED_AT,
            POSTAL_CODE
        FROM step__row_duplicated
),

step__imputed AS (
        SELECT
            ID,
            SUPPLIER_ID,
            COALESCE(NAME, 'UNKNOWN') AS NAME,
            COALESCE(CONTACT_NAME, 'UNKNOWN') AS CONTACT_NAME,
            COALESCE(PHONE, 'UNKNOWN') AS PHONE,
            COALESCE(EMAIL, 'UNKNOWN') AS EMAIL,
            COALESCE(ADRESS, 'UNKNOWN') AS ADRESS,
            COALESCE(COUNTRY, 'UNKNOWN') AS COUNTRY,
            COALESCE(REGION, 'UNKNOWN') AS REGION,
            COALESCE(CITY, 'UNKNOWN') AS CITY,
            COALESCE(POSTAL_CODE, '0') AS POSTAL_CODE,
            COALESCE(UPLOADED_AT, '1900-01-01') AS UPLOADED_AT,
            COALESCE(UPDATED_AT, '1900-01-01') AS UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__avoid_space
),
step__standardized AS(
        SELECT
            LOWER(ID) AS ID,
            LOWER(SUPPLIER_ID) AS SUPPLIER_ID,
            INITCAP(NAME) AS NAME,
            INITCAP(CONTACT_NAME) AS CONTACT_NAME,
            LOWER(PHONE) AS PHONE,
            LOWER(EMAIL) AS EMAIL,
            LOWER(ADRESS) AS ADRESS,
            INITCAP(COUNTRY) AS COUNTRY,
            INITCAP(REGION) AS REGION,
            INITCAP(CITY) AS CITY,
            POSTAL_CODE,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__imputed
),

step__converted AS (
        SELECT
            ID::VARCHAR(250) AS ID,
            SUPPLIER_ID::VARCHAR(250) AS SUPPLIER_ID,
            NAME::VARCHAR(100) AS NAME,
            CONTACT_NAME::VARCHAR(100) AS CONTACT_NAME,
            PHONE::VARCHAR(50) AS PHONE,
            EMAIL::VARCHAR(255) AS EMAIL,
            ADRESS::VARCHAR(255) AS ADRESS,
            COUNTRY::VARCHAR(100) AS COUNTRY,
            REGION::VARCHAR(50) AS REGION,
            CITY::VARCHAR(100) AS CITY,
            POSTAL_CODE::NUMBER(5,0)  AS POSTAL_CODE,
            UPLOADED_AT::TIMESTAMP_NTZ AS UPLOADED_AT,
            UPDATED_AT::TIMESTAMP_NTZ AS UPDATED_AT, 
            SOURCE::VARCHAR(25) AS SOURCE,
            EVENT_TYPE::VARCHAR(25) AS EVENT_TYPE
        FROM step__standardized
),

step__renamed AS (
        SELECT
            ID,
            SUPPLIER_ID,
            NAME,
            CONTACT_NAME,
            PHONE,
            EMAIL,
            ADRESS AS SUPPLIER_ADDRESS,
            COUNTRY,
            REGION,
            CITY,
            POSTAL_CODE,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__converted
),

final AS(
    SELECT
            ID,
            SUPPLIER_ID,
            NAME,
            CONTACT_NAME,
            PHONE,
            EMAIL,
            SUPPLIER_ADDRESS,
            COUNTRY,
            REGION,
            CITY,
            POSTAL_CODE,
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
