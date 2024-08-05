-- Source
WITH src_customers AS (
    SELECT {{ get_columns_by_relation(ref("stg_customers"), ["NAME"])  }}, NAME AS CUSTOMER_NAME FROM {{ref("stg_customers")}}
),

src_orders AS (
    SELECT {{ get_columns_by_relation(ref("stg_orders")) }} FROM {{ref("stg_orders")}}
),
-- Referential
ref_country AS (
    SELECT {{ get_columns_by_relation(ref("country_ref")) }} FROM {{ref("country_ref")}}
),
-- Transformation
step__calculate_new_fields AS ( -- REF = [RG_001, RG_004]
    SELECT {{ get_columns_by_relation(ref("stg_customers"), ["NAME"]) }}, 
           split_part(CUSTOMER_NAME, ' ', 0) AS FIRST_NAME,
           split_part(CUSTOMER_NAME, ' ', 1) AS LAST_NAME,
           r.alpha_2 AS COUNTRY_CODE,
           FLOOR(DATEDIFF(DAY, BIRTHDAY, CURRENT_DATE) / 365.25) AS AGE
    
    FROM src_customers c
    LEFT JOIN ref_country r 
    ON UPPER(c.COUNTRY) = UPPER(r.name)    
),
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
step__rename AS (
        SELECT * FROM step__calculate_new_fields
),
final AS(
SELECT * FROM step__rename
)
SELECT * FROM final
