{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ID', 'STORE_ID']) }} AS ID,
    STORE_ID,
    NAME AS STORE_NAME,
    PHONE AS STORE_PHONE,
    EMAIL AS STORE_EMAIL,
    STORE_ADDRESS,
    COUNTRY,
    REGION,
    CITY,
    POSTAL_CODE
FROM
    {{ ref('in_stores') }}