WITH source AS (
    SELECT  
            ID,
            SALES_CHANNEL_ID,
            NAME,
            DESCRIPTION,
            IS_ACTIVE,
            CREATED_AT,
            UPLOADED_AT,
            UPDATED_AT
FROM {{ ref('trans__sales_channel') }}
)

    SELECT  
            ID,
            SALES_CHANNEL_ID,
            NAME,
            DESCRIPTION,
            IS_ACTIVE,
            CREATED_AT,
            UPLOADED_AT,
            UPDATED_AT
        FROM source
