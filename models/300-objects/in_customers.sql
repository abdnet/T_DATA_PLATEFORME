

WITH source AS (
    SELECT  
            ID,
            CUSTOMER_ID,
            FIRST_NAME,
            LAST_NAME,
            SEX,
            EMAIL,
            AGE,
            BIRTHDAY,
            COUNTRY,
            COUNTRY_CODE,
            REGION,
            CITY,
            POSTAL_CODE,
            SOURCE,
            EVENT_TYPE,
            UPLOADED_AT
FROM {{ ref('trans__customers') }}
)


    SELECT  
            ID,
            CUSTOMER_ID,
            FIRST_NAME,
            LAST_NAME,
            SEX,
            EMAIL,
            AGE,
            BIRTHDAY,
            COUNTRY,
            COUNTRY_CODE,
            REGION,
            CITY,
            POSTAL_CODE
            -- SOURCE,
            -- EVENT_TYPE,
            -- UPLOADED_AT
        FROM source



-- step__calcul_customer_order_stats AS (
--         SELECT
--             CUSTOMER_ID,
--             min(ORDER_DATE) AS FIRST_ORDER,
--             max(ORDER_DATE) AS LAST_ORDER,
--             count(CUSTOMER_ID) AS TOTAL_ORDERS
--         FROM src_orders
--         GROUP BY 1
-- ),
-- step__add_stat_columns AS (
--         SELECT s.*, c.FIRST_ORDER, c.LAST_ORDER, c.TOTAL_ORDERS
--         FROM step__calculate_new_fields AS s
--         LEFT JOIN step__calcul_customer_order_stats AS c
--         ON s.CUSTOMER_ID = c.CUSTOMER_ID
-- ),