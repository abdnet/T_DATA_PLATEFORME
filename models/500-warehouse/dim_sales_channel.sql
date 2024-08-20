{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ID', 'SALES_CHANNEL_ID']) }} AS ID,
    SALES_CHANNEL_ID,
    NAME AS SALES_CHANNEL_NAME,
    DESCRIPTION,
    IS_ACTIVE,
    CREATED_AT
FROM
    {{ ref('in_sales_channel') }}