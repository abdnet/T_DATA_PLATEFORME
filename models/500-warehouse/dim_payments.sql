{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ID', 'PAYMENT_ID', 'ORDER_ID']) }} AS ID,
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
FROM
    {{ ref('in_payments') }}