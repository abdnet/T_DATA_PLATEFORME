{% snapshot item_history %}

{{
    config(
      target_schema='DPF_STG_SNAPSHOT_DEV',
      unique_key='ITEM_ID',
      strategy='timestamp',
      updated_at='UPDATED_AT',
    )
}}

select * from  {{ ref('stg_items') }}

{% endsnapshot %}