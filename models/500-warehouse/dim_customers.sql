{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['ID', 'CUSTOMER_ID']) }} AS ID,
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    SEX,
    EMAIL,
    AGE,
    BIRTHDAY,
    COUNTRY,
    COUNTRY_CODE,
    REGION,
    CITY,
    POSTAL_CODE
FROM
    {{ ref('in_customers') }}