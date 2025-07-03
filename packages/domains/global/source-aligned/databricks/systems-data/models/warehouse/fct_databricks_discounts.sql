WITH fct_databricks_discounts AS (
  SELECT 
    id
    , discount 
    , valid_from 
    , valid_to
  FROM {{ ref('stg_databricks_discounts') }}
)
SELECT * FROM fct_databricks_discounts
