{{ config(
    tags=["PIPELINE_CUSTOMER"]
) }}

-- Source
WITH src_customers AS (
    SELECT {{ get_columns_by_relation(ref("customer_history"), ["NAME"])  }},
            NAME AS CUSTOMER_NAME 
    FROM {{ref("customer_history")}} 
    WHERE DBT_VALID_TO IS NULL
),

-- Referential
ref_country AS (
    SELECT {{ get_columns_by_relation(ref("country_ref")) }} 
    FROM {{ref("country_ref")}}
),
-- Transformation
step__calculate_new_fields AS ( -- REF = [RG_001, RG_004, RG-005]
    SELECT {{ get_columns_by_relation(ref("customer_history"), ["NAME"]) }},
           split_part(CUSTOMER_NAME, ' ', 1) AS FIRST_NAME,
           split_part(CUSTOMER_NAME, ' ', 2) AS LAST_NAME,
           r.alpha_2 AS COUNTRY_CODE,
           FLOOR(DATEDIFF(DAY, BIRTHDAY, CURRENT_DATE) / 365.25) AS AGE

    FROM src_customers c
    LEFT JOIN ref_country r
    ON UPPER(c.COUNTRY) = UPPER(r.name)
),

step__rename AS (
        SELECT * FROM step__calculate_new_fields
),
final AS(
SELECT * FROM step__rename
)
SELECT * FROM final
