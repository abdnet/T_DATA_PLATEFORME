{% set supplier_source = ref("supplier_history") %}

WITH suppliers AS (
    SELECT {{ get_columns_by_relation(supplier_source)  }}
    FROM {{ supplier_source }} 
    WHERE DBT_VALID_TO IS NULL
),

step__derived AS (
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
        UPDATED_AT
    FROM suppliers
),

final AS(
    SELECT * FROM step__derived
)

SELECT *
FROM final
