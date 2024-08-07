{{ config(
    materialized = 'view',
    unique_key = 'CUSTOMER_ID'
) }}


WITH source AS (
        SELECT
            {{get_columns_by_relation(ref("raw_customers"))}}
        FROM {{ ref('raw_customers') }}
    ),

step__valid_row AS(
        SELECT
            ID,
            CUSTOMER_ID,
            NAME,
            SEX,
            EMAIL,
            COUNTRY,
            REGION,
            CITY,
            POSTAL_CODE,
            BIRTHDAY,
            UPLOADED_AT,
            SOURCE,
            EVENT_TYPE
        FROM source
        WHERE  {{ not_null_proportion(['CUSTOMER_ID', 'NAME', 'EMAIL','BIRTHDAY'], 0.8) }}
    ),

step__row_duplicated AS(
           SELECT {{get_columns_by_relation(ref("raw_customers"))}},
           ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY UPLOADED_AT DESC) AS rn
           FROM step__valid_row
           QUALIFY rn = 1
),

step__avoid_space AS(
        SELECT
            {{avoid_spaces(ref("raw_customers"),['BIRTHDAY','UPLOADED_AT','POSTAL_CODE'])}},
            BIRTHDAY,
            UPLOADED_AT,
            POSTAL_CODE
        FROM step__row_duplicated
),

step__imputed AS (
        SELECT
            ID,
            CUSTOMER_ID,
            COALESCE(EMAIL, 'UNKNOWN') AS EMAIL,
            COALESCE(NAME, 'UNKNOWN') AS NAME,
            CASE
                WHEN SEX IN ('F', 'M') THEN SEX
                ELSE 'X' -- Valeur par défaut pour les valeurs invalides
            END AS SEX,
            COALESCE(COUNTRY, 'UNKNOWN') AS COUNTRY,
            COALESCE(REGION, 'UNKNOWN') AS REGION,
            COALESCE(CITY, 'UNKNOWN') AS CITY,
            COALESCE(POSTAL_CODE, '0') AS POSTAL_CODE,
            COALESCE(BIRTHDAY, '1900-01-01') AS BIRTHDAY,
            COALESCE(UPLOADED_AT, '1900-01-01') AS UPLOADED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__avoid_space
),
step__standardized AS(
        SELECT
            LOWER(ID) AS ID,
            LOWER(CUSTOMER_ID) AS CUSTOMER_ID,
            INITCAP(NAME) AS NAME,
            SEX,
            LOWER(EMAIL) AS EMAIL,
            INITCAP(COUNTRY) AS COUNTRY,
            INITCAP(REGION) AS REGION,
            INITCAP(CITY) AS CITY,
            POSTAL_CODE,
            TO_DATE(BIRTHDAY) AS BIRTHDAY,
            TO_DATE(UPLOADED_AT) AS UPLOADED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__imputed
),

step__converted AS (
        SELECT
            ID::VARCHAR(250) AS ID,
            CUSTOMER_ID::VARCHAR(250) AS CUSTOMER_ID,
            NAME::VARCHAR(100) AS NAME,
            SEX::VARCHAR(1) AS SEX,
            EMAIL::VARCHAR(255) AS EMAIL,
            COUNTRY::VARCHAR(100) AS COUNTRY,
            REGION::VARCHAR(50) AS REGION,
            CITY::VARCHAR(100) AS CITY,
            POSTAL_CODE::NUMBER(5,0)  AS POSTAL_CODE,
            BIRTHDAY::DATE AS BIRTHDAY,
            UPLOADED_AT::TIMESTAMP AS UPLOADED_AT,
            SOURCE::VARCHAR(25) AS SOURCE,
            EVENT_TYPE::VARCHAR(25) AS EVENT_TYPE

        FROM step__standardized
),

step__renamed AS (
        SELECT
            ID,
            CUSTOMER_ID,
            NAME,
            SEX,
            EMAIL,
            COUNTRY,
            REGION,
            CITY,
            POSTAL_CODE,
            BIRTHDAY,
            UPLOADED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__converted
),

final AS(
    SELECT
            ID,
            CUSTOMER_ID,
            NAME,
            SEX,
            EMAIL,
            COUNTRY,
            REGION,
            CITY,
            POSTAL_CODE,
            BIRTHDAY,
            UPLOADED_AT,
            SOURCE,
            EVENT_TYPE
        FROM step__renamed
)


select * from final
