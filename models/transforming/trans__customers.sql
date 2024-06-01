
-- SOURCE
with
    source as (select * from {{ ref( "stg_customers") }}),
    orders as (select * from {{ ref("stg_orders") }}),

    stats_by_customer as (select * from {{ ref("trans_orders_calcul_stat") }}),

    -- END SOURCES
    -- Transformations
    step_renaming as (

        select
            id as customer_id,
            {{ dbt.split_part("name", "' '", 1) }} as first_name,
            {{ dbt.split_part("name", "' '", 2) }} as last_name,
            (select current_timestamp)::timestamp as loaded_at
        from source
    ),

    step_join as (

        select c.*, stats.*
        from step_renaming as c
        right join stats_by_customer as stats on id = c.customer_id
    ),

    -- add a random date column
    row_numbers as (
        select c.*, row_number() over (order by customer_id) as row_num
        from step_join as c
    ),
    date_generator as (
        select
            row_num,
            dateadd('week', row_num*2, '1950-01-01') as date_day
        from row_numbers
    ),
    joined_data as (
        select c.*,
         d.date_day as birthday
        from row_numbers as c
        join date_generator as d on c.row_num = d.row_num
    ),
    -- END tranformations
    -- return 
    final as (
        select 
           customer_id as id,
           first_name,
           last_name,
           birthday,
           first_order,
           last_order,
           total_orders 
         from joined_data
    -- order by total_orders
    )

select *
from final
order by random()