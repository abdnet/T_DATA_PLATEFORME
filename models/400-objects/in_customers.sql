WITH source AS (
    SELECT  
            ID,
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
            POSTAL_CODE,
            UPLOADED_AT,
            UPDATED_AT
    FROM {{ ref('trans__customers') }}
)

    SELECT  
            ID,
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
            POSTAL_CODE,
            UPLOADED_AT,
            UPDATED_AT
    FROM source
