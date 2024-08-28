{{ config(materialized='table') }}

SELECT
    id AS supplier_key,
    supplier_id,
    name AS supplier_name,
    contact_name,
    phone,
    email,
    supplier_address,
    country,
    region,
    city,
    postal_code,
    uploaded_at,
    updated_at
FROM
    {{ ref('in_suppliers') }}