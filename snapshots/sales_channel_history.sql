{% snapshot sales_channel_history %}

{{
    config(
      target_schema='DPF_STG_SNAPSHOT_DEV',
      unique_key='SALES_CHANNEL_ID',
      strategy='timestamp',
      updated_at='UPDATED_AT',
    )
}}

select * from  {{ ref('stg_sales_channel') }}

{% endsnapshot %}