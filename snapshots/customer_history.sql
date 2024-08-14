{% snapshot customer_history %}

{{
    config(
      target_schema='DPF_STG_SNAPSHOT_DEV',
      unique_key='CUSTOMER_ID',
      strategy='timestamp',
      updated_at='UPDATED_AT',
    )
}}

select * from  {{ ref('stg_customers') }}

{% endsnapshot %}