{% snapshot product_history %}

{{
    config(
      target_schema='DPF_SCH_SNAPSHOT',  
      unique_key='PRODUCT_ID',  
      strategy='check',  
      check_cols=['UNIT_PRICE']

    )
}}

SELECT
    ID,
    PRODUCT_ID,
    NAME,
    CATEGORY,
    SUB_CATEGORY,
    BRAND,
    COLOR,
    SIZE,
    UNIT_PRICE,
    UPLOADED_AT,
    UPDATED_AT,
    SOURCE,
    EVENT_TYPE
FROM {{ ref('stg_products') }}  

{% endsnapshot %}