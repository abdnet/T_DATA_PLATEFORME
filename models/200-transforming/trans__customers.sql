-- Source
WITH src_customers AS (
    SELECT {{ get_columns_by_relation(ref("stg_customers"), ["NAME"])  }}, NAME AS CUSTOMER_NAME FROM ref("stg_customers")
),

src_orders AS (
    SELECT {{ get_columns_by_relation(ref("stg_orders")) }} FROM ref("stg_orders")
),

step_avoid_spaces AS (
    SELECT {{ avoid_spaces(ref("stg_customers"), ["POSTAL_CODE","UPLOADED_AT","NAME"]) }},
           POSTAL_CODE,
           CUSTOMER_NAME,
           UPLOADED_AT
    FROM src_customers
),

step_split_name AS (
    SELECT {{ get_columns_by_relation(ref("stg_customers"), ["CUSTOMER_NAME"]) }}, 
           split_part(CUSTOMER_NAME, ' ', 0) AS FIRST_NAME,
           split_part(CUSTOMER_NAME, ' ', 1) AS LAST_NAME
    FROM avoid_spaces     
),

step_calcul_customerorder_stats as (

        select
            customer_id,
            min(ordered_at) as first_order,
            max(ordered_at) as last_order,
            count(customer_id) as total_orders
        from src_orders
        group by 1
)

SELECT * FROM step_split_name