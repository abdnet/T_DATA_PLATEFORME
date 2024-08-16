WITH source AS (
    SELECT  
            ID,
            SUPPLIER_ID,
            NAME,
            CONTACT_NAME,
            PHONE,
            EMAIL,
            SUPPLIER_ADDRESS,
            COUNTRY,
            REGION,
            CITY,
            POSTAL_CODE,
            UPLOADED_AT,
            UPDATED_AT
FROM {{ ref('trans__suppliers') }}
)

    SELECT  
            ID,
            SUPPLIER_ID,
            NAME,
            CONTACT_NAME,
            PHONE,
            EMAIL,
            SUPPLIER_ADDRESS,
            COUNTRY,
            REGION,
            CITY,
            POSTAL_CODE,
            UPLOADED_AT,
            UPDATED_AT
        FROM source
