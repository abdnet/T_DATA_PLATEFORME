{{ config(
    materialized = 'incremental',
    unique_key = 'PRODUCT_ID'
) }}

WITH source AS (
        SELECT
            {{get_columns_by_relation(ref("raw_products"))}}
        FROM {{ ref('raw_products') }}
),

step__row_duplicated AS(
   
           {{ dbt_utils.deduplicate(
                relation=ref("raw_products"),
                partition_by='PRODUCT_ID',
                order_by="UPLOADED_AT desc",
            )
         }}
),

step__valid_row AS(
        SELECT
            ID,
            PRODUCT_ID,
            NAME,
            CATEGORY,
            SUB_CATEGORY,
            BRAND,
            COLOR,
            SIZE,
            UNIT_PRICE,
            UPLOADED_AT,
            SOURCE,
            UPDATED_AT,
            EVENT_TYPE
        FROM step__row_duplicated
        WHERE  {{ not_null_proportion(['PRODUCT_ID', 'NAME', 'UNIT_PRICE','CATEGORY'], 0.8) }}
),

step__avoid_space AS(
        SELECT
            {{avoid_spaces(ref("raw_products"),['UNIT_PRICE','UPLOADED_AT','UPDATED_AT'])}},
            UNIT_PRICE,
            UPLOADED_AT,
            UPDATED_AT
        FROM step__valid_row
),

step__imputed AS (
    SELECT 
            ID,
            PRODUCT_ID,
            COALESCE(NAME, 'UNKNOWN') AS NAME,
            COALESCE(CATEGORY, 'UNKNOWN') AS CATEGORY,
            COALESCE(SUB_CATEGORY, 'UNKNOWN') AS SUB_CATEGORY,
            COALESCE(BRAND, 'UNKNOWN') AS BRAND,
            COALESCE(COLOR, 'UNKNOWN') AS COLOR,
            CASE
                WHEN SIZE IN ('XS','S', 'L','M','XL') THEN SIZE
                ELSE 'X' -- Valeur par défaut pour les valeurs invalides
            END AS SIZE,
            COALESCE(UNIT_PRICE, '0') AS UNIT_PRICE,
            COALESCE(UPLOADED_AT, '1900-01-01') AS UPLOADED_AT,
            SOURCE,
            EVENT_TYPE,
            UPDATED_AT

    FROM step__avoid_space
),

step__standardized AS(
    SELECT
            LOWER(ID) AS ID,
            LOWER(PRODUCT_ID) AS PRODUCT_ID,
            INITCAP(NAME) AS NAME,
            UPPER(SIZE) AS SIZE,
            INITCAP(CATEGORY) AS CATEGORY,
            INITCAP(SUB_CATEGORY) AS SUB_CATEGORY,
            INITCAP(BRAND) AS BRAND,
            INITCAP(COLOR) AS COLOR,
            UNIT_PRICE,
            UPLOADED_AT,
            UPDATED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__imputed
),

step__converted AS (
    SELECT        
            ID::VARCHAR(250) AS ID,
            PRODUCT_ID::VARCHAR(250) AS PRODUCT_ID,
            NAME::VARCHAR(100) AS NAME,
            SIZE::VARCHAR(2) AS SIZE,
            CATEGORY::VARCHAR(255) AS CATEGORY,
            SUB_CATEGORY::VARCHAR(100) AS SUB_CATEGORY,
            BRAND::VARCHAR(250) AS BRAND,
            COLOR::VARCHAR(100) AS COLOR,
            UNIT_PRICE::NUMBER(10,0)  AS UNIT_PRICE,
            UPLOADED_AT::TIMESTAMP_NTZ AS UPLOADED_AT,
            UPDATED_AT::TIMESTAMP_NTZ AS UPDATED_AT, 
            SOURCE::VARCHAR(25) AS SOURCE,
            EVENT_TYPE::VARCHAR(25) AS EVENT_TYPE
    FROM step__standardized
),

step__renamed as (
    SELECT 
            ID ,
            PRODUCT_ID ,
            NAME,
            CATEGORY  ,
            SUB_CATEGORY ,
            BRAND ,
            COLOR ,
            SIZE ,
            UNIT_PRICE ,
            UPLOADED_AT ,
            SOURCE ,
            EVENT_TYPE,
            UPDATED_AT
    
     FROM step__imputed
),

final AS (
    SELECT 
            ID,
            PRODUCT_ID,
            NAME,
            CATEGORY,
            SUB_CATEGORY,
            BRAND,
            COLOR,
            SIZE,
            UNIT_PRICE,
            UPLOADED_AT,
            SOURCE,
            EVENT_TYPE,
            UPDATED_AT,
            TIMESTAMPADD('hour', 9, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) AS DBT_UPDATED_DATE
    FROM step__renamed

)

SELECT * FROM final
{% if is_incremental() %}
where UPLOADED_AT >= (SELECT COALESCE(MAX(DBT_UPDATED_DATE),'1900-01-01') FROM {{ this }})   
OR UPDATED_AT >= (SELECT COALESCE(MAX(DBT_UPDATED_DATE),'1900-01-01') FROM {{ this }})    
{% endif %}