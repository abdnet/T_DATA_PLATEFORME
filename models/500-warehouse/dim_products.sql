{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ID', 'PRODUCT_ID']) }} AS ID,
    PRODUCT_ID,
    NAME AS PRODUCT_NAME,
    CATEGORY AS PRODUCT_CATEGORY,
    SUB_CATEGORY AS PRODUCT_SUB_CATEGORY,
    BRAND,
    COLOR,
    SIZE,
    UNIT_PRICE
FROM
    {{ ref('in_products') }}