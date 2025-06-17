WITH stg_databricks_discounts AS (
  SELECT 
    id
    , discount 
    , valid_from 
    , valid_to
  FROM {{ ref('data_databricks_discounts') }}
)
SELECT * FROM stg_databricks_discounts
