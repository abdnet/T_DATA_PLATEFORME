with source as ( 
    select * from {{ref('stg_customers')}}
)

select * from source