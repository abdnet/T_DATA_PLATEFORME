{% set store_source = ref("store_history") %}

WITH stores AS (
    SELECT {{ get_columns_by_relation(store_source)  }}
    FROM {{ store_source }} 
    WHERE DBT_VALID_TO IS NULL
),

step__derived AS (
    SELECT
        ID,
        STORE_ID,
        NAME,
        PHONE,
        EMAIL,
        STORE_ADDRESS,
        COUNTRY,
        REGION,
        CITY,
        POSTAL_CODE,
        UPLOADED_AT,
        UPDATED_AT
    FROM stores
),

final AS(
    SELECT * FROM step__derived
)

SELECT *
FROM final
