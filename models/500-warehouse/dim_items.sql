{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ID', 'ITEM_ID', 'ORDER_ID', 'PRODUCT_ID']) }} AS ID,
    item_id,
    order_id,
    product_id,
    supplier_id,
    item_quantity,
    uploaded_at,
    updated_at,
    order_id_validity,
    product_id_validity
FROM
    {{ ref('in_items') }}