-- Ce modèle agrège les données de ventes en regroupant par date et catégorie de produit.
-- Il calcule des KPI clés, y compris les ventes totales, les retours, et les statistiques sur les clients nouveaux et existants.
-- Il fournit également les pourcentages de nouveaux clients et de clients existants par rapport au total des clients.

WITH sales_data AS (
    SELECT
        f.ORDER_DATE,
        f.PRODUCT_CATEGORY,
        SUM(f.TOTAL_SALE_AMOUNT) AS TOTAL_SALES,
        SUM(CASE WHEN f.IS_RETURNED = 'Yes' THEN f.TOTAL_SALE_AMOUNT ELSE 0 END) AS TOTAL_RETURNS,
        COUNT(DISTINCT f.ORDER_ID) AS NUMBER_OF_ORDERS,
        COUNT(DISTINCT f.CUSTOMER_ID) AS NUMBER_OF_CUSTOMERS,
        ROUND(AVG(f.DELIVERY_DELAY),2) AS AVG_DELIVERY_DELAY,
        COUNT(DISTINCT CASE WHEN f.IS_FIRST_ORDER = 'Yes' THEN f.CUSTOMER_ID END) AS NUMBER_OF_NEW_CUSTOMERS,
        COUNT(DISTINCT CASE WHEN f.IS_FIRST_ORDER = 'No' THEN f.CUSTOMER_ID END) AS NUMBER_OF_EXISTING_CUSTOMERS
    FROM
        {{ ref('fact_sales') }} f
    GROUP BY
        f.ORDER_DATE,
        f.PRODUCT_CATEGORY
),

step__calculate_pourcent_customer as (

    SELECT
        s.ORDER_DATE,
        s.PRODUCT_CATEGORY,
        s.TOTAL_SALES,
        s.TOTAL_RETURNS,
        s.NUMBER_OF_ORDERS,
        s.NUMBER_OF_CUSTOMERS,
        s.AVG_DELIVERY_DELAY,
        s.NUMBER_OF_NEW_CUSTOMERS,
        s.NUMBER_OF_EXISTING_CUSTOMERS,
        ROUND((s.NUMBER_OF_NEW_CUSTOMERS::FLOAT / s.NUMBER_OF_CUSTOMERS) * 100,2) AS PERCENT_NEW_CUSTOMERS,
        ROUND((s.NUMBER_OF_EXISTING_CUSTOMERS::FLOAT / s.NUMBER_OF_CUSTOMERS) * 100,2) AS PERCENT_EXISTING_CUSTOMERS
    FROM
        sales_data s
    ORDER BY
        s.ORDER_DATE, s.PRODUCT_CATEGORY
),

step_rename as(
    SELECT
        ORDER_DATE,
        PRODUCT_CATEGORY,
        TOTAL_SALES AS TOTAL_SALES_REVENUE,
        TOTAL_RETURNS AS TOTAL_RETURNS_REVENUE,
        NUMBER_OF_ORDERS,
        NUMBER_OF_CUSTOMERS,
        AVG_DELIVERY_DELAY,
        NUMBER_OF_NEW_CUSTOMERS,
        NUMBER_OF_EXISTING_CUSTOMERS,
        PERCENT_NEW_CUSTOMERS,
        PERCENT_EXISTING_CUSTOMERS
    FROM step__calculate_pourcent_customer
),

final as (
    SELECT
        ORDER_DATE,
        PRODUCT_CATEGORY,
        TOTAL_SALES_REVENUE,
        TOTAL_RETURNS_REVENUE,
        NUMBER_OF_ORDERS,
        NUMBER_OF_CUSTOMERS,
        AVG_DELIVERY_DELAY,
        NUMBER_OF_NEW_CUSTOMERS,
        NUMBER_OF_EXISTING_CUSTOMERS,
        PERCENT_NEW_CUSTOMERS,
        PERCENT_EXISTING_CUSTOMERS
    FROM step_rename
)

SELECT * FROM step_rename


