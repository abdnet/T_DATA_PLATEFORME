
WITH orders AS (
    SELECT
        o.ORDER_ID,
        o.CUSTOMER_ID,
        o.STORE_ID,
        o.SALES_CHANNEl_ID,
        o.ORDER_DATE,
        o.DELIVERY_DATE,
        o.RETURN_DATE,
        o.TOTAL_PRICE,
        o.IS_RETURNED,
        o.ORDER_STATUS,
        o.CANCELLATION_STATUS,
        o.IS_FIRST_ORDER,
        o.DELIVERY_DELAY
    FROM {{ ref('dim_orders') }} o
),

items AS (
    SELECT
        i.ITEM_ID,
        i.ORDER_ID,
        i.PRODUCT_ID,
        i.ITEM_QUANTITY
    FROM {{ ref('dim_items') }} i
),

products AS (
    SELECT
        p.PRODUCT_ID,
        p.PRODUCT_NAME,
        p.UNIT_PRICE,
        p.PRODUCT_CATEGORY,
        p.PRODUCT_SUB_CATEGORY
    FROM {{ ref('dim_products') }} p
),

customers AS (
    SELECT
        c.CUSTOMER_ID,
        c.FIRST_NAME AS CUSTOMER_FIRST_NAME,
        c.LAST_NAME AS CUSTOMER_LAST_NAME,
        c.SEX AS CUSTOMER_SEX,
        c.AGE AS CUSTOMER_AGE,
        c.COUNTRY AS CUSTOMER_COUNTRY,
        c.REGION AS CUSTOMER_REGION,
        c.CITY AS CUSTOMER_CITY,
        c.POSTAL_CODE AS CUSTOMER_POSTAL_CODE
    FROM {{ ref('dim_customers') }} c
),


step_join AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['o.ORDER_ID', 'p.PRODUCT_ID']) }} AS SALES_ID,
        o.ORDER_ID,
        o.CUSTOMER_ID,
        p.PRODUCT_ID,
        i.ITEM_ID,
        o.ORDER_DATE,
        o.DELIVERY_DATE,
        o.RETURN_DATE,
        p.PRODUCT_NAME,
        p.PRODUCT_CATEGORY,
        p.PRODUCT_SUB_CATEGORY,
        ROUND(o.TOTAL_PRICE),2 AS TOTAL_ORDER_REVENUE,
        o.IS_RETURNED,
        o.ORDER_STATUS,
        o.CANCELLATION_STATUS,
        o.IS_FIRST_ORDER,
        i.ITEM_QUANTITY AS TOTAL_QUANTITY,
        ROUND((p.UNIT_PRICE * i.ITEM_QUANTITY),2) AS TOTAL_SALE_AMOUNT,
        o.DELIVERY_DELAY,
        o.STORE_ID,
        CASE WHEN o.IS_RETURNED = 'Yes' THEN i.ITEM_QUANTITY ELSE 0 END AS RETURN_RATE
    FROM orders o
    JOIN items i ON o.ORDER_ID = i.ORDER_ID
    JOIN products p ON i.PRODUCT_ID = p.PRODUCT_ID
    GROUP BY ALL
),

step_rename AS (
    SELECT
        SALES_ID,
        ORDER_ID,
        CUSTOMER_ID,
        PRODUCT_ID,
        ITEM_ID,
        ORDER_DATE,
        DELIVERY_DATE,
        RETURN_DATE,
        PRODUCT_NAME,
        PRODUCT_CATEGORY,
        PRODUCT_SUB_CATEGORY,
        TOTAL_ORDER_REVENUE,
        IS_RETURNED,
        ORDER_STATUS,
        CANCELLATION_STATUS,
        IS_FIRST_ORDER,
        TOTAL_QUANTITY,
        TOTAL_SALE_AMOUNT,
        DELIVERY_DELAY,
        STORE_ID,
        RETURN_RATE
    FROM step_join 
    
),

final AS (
    SELECT
        SALES_ID,
        ORDER_ID,
        CUSTOMER_ID,
        PRODUCT_ID,
        ITEM_ID,
        ORDER_DATE,
        DELIVERY_DATE,
        RETURN_DATE,
        PRODUCT_NAME,
        PRODUCT_CATEGORY,
        PRODUCT_SUB_CATEGORY,
        TOTAL_ORDER_REVENUE,
        IS_RETURNED,
        ORDER_STATUS,
        CANCELLATION_STATUS,
        IS_FIRST_ORDER,
        TOTAL_QUANTITY,
        TOTAL_SALE_AMOUNT,
        DELIVERY_DELAY,
        STORE_ID,
        RETURN_RATE
    FROM step_rename 
)

SELECT * from final

