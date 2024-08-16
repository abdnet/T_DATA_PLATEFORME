{% set item_source = ref('item_history') %}
{% set order_source = ref('trans__orders') %}
{% set product_source = ref('trans__products') %}

WITH source AS (
    SELECT {{ dbt_utils.star(item_source) }}
    FROM {{ item_source }}
),

order_s AS (
    SELECT ORDER_ID AS ORDER_C
    FROM {{ order_source }}
),

product AS (
    SELECT PRODUCT_ID AS PRODUCT 
    FROM {{ product_source }}
),

step__validate_order AS (
        SELECT {{ dbt_utils.star(item_source) }},
        CASE 
            WHEN o.ORDER_C IS NOT NULL THEN true
            ELSE false
        END AS ORDER_ID_VALIDITY
    FROM source AS s
    LEFT JOIN order_s o ON s.ORDER_ID = o.ORDER_C
),

step__validate_product AS (
        SELECT {{ dbt_utils.star(item_source) }},
        ORDER_ID_VALIDITY,
        CASE 
            WHEN p.PRODUCT IS NOT NULL THEN true
            ELSE false
        END AS PRODUCT_ID_VALIDITY
    FROM step__validate_order AS o
    LEFT JOIN product p ON o.PRODUCT_ID = p.PRODUCT
),

-- Final data output
final AS (
    SELECT
        *
    FROM 
        step__validate_product
    -- Add any additional business logic here
)

-- Select final transformed data
SELECT 
    * 
FROM 
    final
