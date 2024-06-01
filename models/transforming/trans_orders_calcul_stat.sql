WITH source AS(

    select * from {{ ref('stg_orders') }}

),

step_calcul_customerorder_stats as (

        select
            customer as id,
            min(ordered_at) as first_order,
            max(ordered_at) as last_order,
            count(customer) as total_orders
        from source
        group by 1
),

final AS (
    select *  
    from step_calcul_customerorder_stats 
)

select * from final