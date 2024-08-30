{% snapshot item_history %}

{{
    config(
      target_schema='DPF_SCH_SNAPSHOT',
      unique_key='ITEM_ID',
      strategy='timestamp',
      updated_at='UPDATED_AT',
    )
}}

select * from  {{ ref('stg_items') }}

{% endsnapshot %}