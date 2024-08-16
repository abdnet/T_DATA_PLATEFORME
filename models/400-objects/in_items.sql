WITH source AS (
    SELECT  
        ID, 
        ITEM_ID,
        ORDER_ID,
        PRODUCT_ID, 
        SUPPLIER_ID,
        ITEM_QUANTITY,
        UPLOADED_AT,
        UPDATED_AT, 
        ORDER_ID_VALIDITY,
        PRODUCT_ID_VALIDITY
    FROM {{ ref('trans__items') }}
)

    SELECT  
        ID, 
        ITEM_ID,
        ORDER_ID,
        PRODUCT_ID, 
        SUPPLIER_ID,
        ITEM_QUANTITY,
        UPLOADED_AT,
        UPDATED_AT, 
        ORDER_ID_VALIDITY,
        PRODUCT_ID_VALIDITY
    FROM source
