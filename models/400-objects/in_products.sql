WITH source AS (
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
FROM {{ ref('trans__products') }}
)

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
        FROM source
