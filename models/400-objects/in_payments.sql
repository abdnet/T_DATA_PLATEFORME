WITH source AS (
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
        ORDER_ID_VALIDITY
    FROM {{ ref('trans__payments') }}
)

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
        ORDER_ID_VALIDITY
    FROM source
