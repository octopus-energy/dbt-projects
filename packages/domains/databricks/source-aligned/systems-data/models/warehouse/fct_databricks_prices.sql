WITH fct_databricks_prices AS (
  SELECT
    price_id
    , account_id
    , price_start_time
    , price_end_time
    , sku_name
    , cloud
    , currency_code
    , usage_unit
    , price
  FROM {{ ref('stg_databricks_prices') }}
)
SELECT * FROM fct_databricks_prices
