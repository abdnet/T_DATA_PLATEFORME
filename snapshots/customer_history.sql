{% snapshot customer_history %}

{{
    config(
      target_schema='DPF_SCH_SNAPSHOT',
      unique_key='CUSTOMER_ID',
      strategy='timestamp',
      updated_at='UPDATED_AT',
    )
}}

select * from  {{ ref('stg_customers') }}

{% endsnapshot %}