{% set order_source = ref('order_history') %}
{% set customer_source = ref('trans__customers') %}
{% set store_source = ref('stg_stores') %}
{% set sales_channel_source = ref('stg_sales_channel') %}

WITH source AS (
    SELECT {{ dbt_utils.star(order_source) }}
    FROM {{ order_source }}
),

customer AS (
    SELECT CUSTOMER_ID AS CUSTOMER 
    FROM {{ customer_source }}
),

sale_channel AS (
    SELECT SALES_CHANNEL_ID AS SALES_CHANNEL 
    FROM {{ sales_channel_source }}
),

store AS (
    SELECT STORE_ID AS STORE
    FROM {{ store_source }}

),

step__validate_customer AS (
        SELECT {{ dbt_utils.star(order_source) }},
        CASE 
            WHEN c.CUSTOMER IS NOT NULL THEN true
            ELSE false
        END AS CUSTOMER_ID_VALIDITY
    FROM source AS s
    LEFT JOIN customer c ON s.CUSTOMER_ID = c.CUSTOMER
),

step__validate_SALESCHANNEL AS (
        SELECT {{ dbt_utils.star(order_source) }},
        CUSTOMER_ID_VALIDITY,
        CASE 
            WHEN c.SALES_CHANNEL IS NOT NULL THEN true
            ELSE false
        END AS SALES_CHANNEL_ID_VALIDITY
    FROM step__validate_customer AS o
    LEFT JOIN sale_channel c ON o.SALES_CHANNEL_ID = c.SALES_CHANNEL
),

step__validate_STORE AS (
        SELECT {{ dbt_utils.star(order_source) }},
        CUSTOMER_ID_VALIDITY,
        SALES_CHANNEL_ID_VALIDITY,
        CASE 
            WHEN c.STORE IS NOT NULL THEN true
            ELSE false
        END AS STORE_ID_VALIDITY
    FROM step__validate_SALESCHANNEL AS o
    LEFT JOIN store c ON o.STORE_ID = c.STORE
),

step__calculate_new_fields AS(
    
    SELECT last_step.*,
    DATEDIFF(DAY, ORDER_DATE, DELIVERY_DATE) AS DELIVERY_DELAY, -- if DELIVERY_DATE is null 
    CASE 
        WHEN RETURN_DATE IS NOT NULL THEN 'Yes'
        ELSE 'No'
        END AS IS_RETURNED,
    CASE 
        WHEN RETURN_DATE IS NOT NULL THEN 'Returned'
        WHEN DELIVERY_DATE IS NOT NULL THEN 'Delivered'
        ELSE 'Pending'
        END AS ORDER_STATUS,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY ORDER_DATE) = 1 THEN true
        ELSE false
    END AS IS_FIRST_ORDER,
    CASE
        WHEN RETURN_DATE IS NOT NULL AND DELIVERY_DATE IS NULL THEN 'Pending Cancellation'
        WHEN RETURN_DATE IS NOT NULL AND DELIVERY_DATE IS NOT NULL THEN 'Cancelled'
        ELSE 'Not Cancelled'
    END AS CANCELLATION_STATUS

    FROM step__validate_STORE AS last_step
),


filtered as (
    SELECT *
    from step__calculate_new_fields
    WHERE CUSTOMER_ID_VALIDITY = true
),

fianl AS( SELECT * FROM filtered)

SELECT * FROM fianl

