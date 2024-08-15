WITH src_products AS (
    SELECT {{ get_columns_by_relation(ref("products_history"))  }}
    FROM {{ref("products_history")}} 
    WHERE DBT_VALID_TO IS NULL
),

step__rename AS (
    SELECT * FROM src_products
),

final AS(
    SELECT * FROM step__rename
)

SELECT *
FROM final
