{% set payment_source = ref('payment_history') %}
{% set order_source = ref('trans__orders') %}

WITH source AS (
    SELECT {{ dbt_utils.star(payment_source) }}
    FROM {{ payment_source }}
    WHERE DBT_VALID_TO IS NULL
),

order_s AS (
    SELECT ORDER_ID AS ORDER_C
    FROM {{ order_source }}
),

step__validate_order AS (
        SELECT {{ dbt_utils.star(payment_source) }},
        CASE 
            WHEN o.ORDER_C IS NOT NULL THEN true
            ELSE false
        END AS ORDER_ID_VALIDITY
    FROM source AS s
    LEFT JOIN order_s o ON s.ORDER_ID = o.ORDER_C
),

-- Final data output
final AS (
    SELECT
        *
    FROM 
        step__validate_order
    -- Add any additional business logic here
)

-- Select final transformed data
SELECT 
    * 
FROM 
    final
