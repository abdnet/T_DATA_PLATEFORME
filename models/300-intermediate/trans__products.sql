{% set product_source = ref("product_history") %}

WITH products AS (
    SELECT {{ get_columns_by_relation(product_source)  }}
    FROM {{ product_source }} 
    WHERE DBT_VALID_TO IS NULL
),

step__derived AS (
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
        UPDATED_AT
    FROM products
),

final AS(
    SELECT * FROM step__derived
)

SELECT *
FROM final
