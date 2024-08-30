{% snapshot store_history %}

{{
    config(
      target_schema='DPF_SCH_SNAPSHOT',
      unique_key='STORE_ID',
      strategy='timestamp',
      updated_at='UPDATED_AT',
    )
}}

select * from  {{ ref('stg_stores') }}

{% endsnapshot %}