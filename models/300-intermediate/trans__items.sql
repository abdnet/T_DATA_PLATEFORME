{% set item_source = ref('item_history') %}
{% set order_source = ref('trans__orders') %}
--{% set product_source = ref('trans__products') %}
--{% set supplier_source = ref('trans__suppliers') %}

WITH source AS (
    SELECT {{ dbt_utils.star(item_source) }}
    FROM {{ item_source }}
),

order AS (
    SELECT ORDER_ID AS ORDER
    FROM {{ order_source }}
),

product AS (
    SELECT PRODUCT_ID AS PRODUCT 
    FROM {{ product_source }}
),

supplier AS (
    SELECT SUPPLIER_ID AS SUPPLIER
    FROM {{ supplier_source }}
),

step__validate_order AS (
        SELECT {{ dbt_utils.star(item_source) }},
        CASE 
            WHEN c.ORDER IS NOT NULL THEN true
            ELSE false
        END AS ORDER_ID_VALIDITY
    FROM source AS s
    LEFT JOIN order o ON s.ORDER_ID = o.ORDER
),

step__validate_product AS (
        SELECT {{ dbt_utils.star(item_source) }},
        PRODUCT_ID_VALIDITY,
        CASE 
            WHEN c.PRODUCT_ID IS NOT NULL THEN true
            ELSE false
        END AS PRODUCT_ID_VALIDITY
    FROM step__validate_order AS o
    LEFT JOIN product p ON o.PRODUCT_ID = p.PRODUCT
),

-- step__validate_SUPPLIER AS (
--         SELECT {{ dbt_utils.star(item_source) }},
--         ORDER_ID_VALIDITY,
--         PRODUCT_ID_VALIDITY,
--         CASE 
--             WHEN c.SUPPLIER IS NOT NULL THEN true
--             ELSE false
--         END AS SUPPLIER_ID_VALIDITY
--     FROM step__validate_product AS p
--     LEFT JOIN supplier s ON p.SUPPLIER_ID = s.SUPPLIER
-- ),

step_base AS (
    SELECT
        ID,
        ITEM_ID,
        ORDER_ID,
        PRODUCT_ID,
        SUPPLIER_ID,
        ITEM_QUANTITY,  
        UPLOADED_AT,
        UPDATED_AT
    FROM step__validate_product
),

-- Create derived columns
step_derived AS (
    SELECT
        b.*,
        o.ORDER_DATE AS ORDER_DATE,  
    FROM 
        step_base b
    JOIN stg_orders o
    ON b.ORDER_ID = o.ORDER_ID     
),

-- Filter to include only recent data (e.g., last 6 months)
step_filtered AS (
    SELECT
        *
    FROM 
        step_derived
    WHERE 
        UPDATED_AT >= DATEADD(month, -6, CURRENT_DATE)  -- Filter for recent data
),

-- Final data output
final AS (
    SELECT
        ID,
        ITEM_ID,
        ORDER_ID,
        PRODUCT_ID,
        SUPPLIER_ID,
        ITEM_QUANTITY,
        ORDER_DATE,
        UPLOADED_AT,
        UPDATED_AT
    FROM 
        step_filtered
    -- Add any additional business logic here
)

-- Select final transformed data
SELECT 
    * 
FROM 
    final;
