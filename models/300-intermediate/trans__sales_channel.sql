{% set sales_channel_source = ref("sales_channel_history") %}

WITH sales_channel AS (
    SELECT {{ get_columns_by_relation(sales_channel_source)  }}
    FROM {{ sales_channel_source }} 
    WHERE DBT_VALID_TO IS NULL
),

step__derived AS (
    SELECT
        ID,
        SALES_CHANNEL_ID,
        NAME,
        DESCRIPTION,
        IS_ACTIVE,
        CREATED_AT,
        UPLOADED_AT,
        UPDATED_AT
    FROM sales_channel
),

final AS(
    SELECT * FROM step__derived
)

SELECT *
FROM final
