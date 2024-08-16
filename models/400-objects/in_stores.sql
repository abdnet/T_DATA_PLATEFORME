WITH source AS (
    SELECT  
            ID,
            STORE_ID,
            NAME,
            PHONE,
            EMAIL,
            STORE_ADDRESS,
            COUNTRY,
            REGION,
            CITY,
            POSTAL_CODE,
            UPLOADED_AT,
            UPDATED_AT
FROM {{ ref('trans__stores') }}
)

    SELECT  
            ID,
            STORE_ID,
            NAME,
            PHONE,
            EMAIL,
            STORE_ADDRESS,
            COUNTRY,
            REGION,
            CITY,
            POSTAL_CODE,
            UPLOADED_AT,
            UPDATED_AT
        FROM source
