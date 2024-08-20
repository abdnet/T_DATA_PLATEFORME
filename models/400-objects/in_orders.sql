WITH source AS (
    SELECT  
            ID,
            CUSTOMER_ID,
            ORDER_ID,
            STORE_ID,
            SALES_CHANNEL_ID,
            ORDER_DATE,
            DELIVERY_DATE,
            RETURN_DATE,
            TOTAL_PRICE,
            UPLOADED_AT,
            UPDATED_AT, 
            CUSTOMER_ID_VALIDITY,
            SALES_CHANNEL_ID_VALIDITY,
            STORE_ID_VALIDITY,
            DELIVERY_DELAY,
            IS_RETURNED,
            ORDER_STATUS,
            IS_FIRST_ORDER,
            CANCELLATION_STATUS
    FROM {{ ref('trans__orders') }}
)

    SELECT  
            ID,
            CUSTOMER_ID,
            ORDER_ID,
            STORE_ID,
            SALES_CHANNEL_ID,
            ORDER_DATE,
            DELIVERY_DATE,
            RETURN_DATE,
            TOTAL_PRICE,
            UPLOADED_AT,
            UPDATED_AT, 
            CUSTOMER_ID_VALIDITY,
            SALES_CHANNEL_ID_VALIDITY,
            STORE_ID_VALIDITY,
            DELIVERY_DELAY,
            IS_RETURNED,
            ORDER_STATUS,
            IS_FIRST_ORDER,
            CANCELLATION_STATUS
    FROM source
