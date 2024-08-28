{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ID', 'ORDER_ID', 'CUSTOMER_ID']) }} AS ID,
    CUSTOMER_ID,
    ORDER_ID,
    STORE_ID,
    SALES_CHANNEL_ID,
    ORDER_DATE,
    DELIVERY_DATE,
    RETURN_DATE,
    TOTAL_PRICE,
    CUSTOMER_ID_VALIDITY,
    SALES_CHANNEL_ID_VALIDITY,
    STORE_ID_VALIDITY,
    DELIVERY_DELAY,
    IS_RETURNED,
    ORDER_STATUS,
    IS_FIRST_ORDER,
    CANCELLATION_STATUS
FROM
    {{ ref('in_orders') }}