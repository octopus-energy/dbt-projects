WITH stg_databricks_prices AS (
  SELECT
    -- noqa: disable=RF03
    {{ dbt_utils.generate_surrogate_key([
        'account_id',
        'price_start_time',
        'price_end_time',
        'sku_name',
        'cloud',
        'currency_code',
        'pricing'
    ]) }} AS price_id
    , account_id
    , price_start_time
    , price_end_time
    , sku_name
    , cloud
    , currency_code
    , usage_unit
    , CAST(pricing.default AS DOUBLE) AS price
    -- noqa: enable=RF03
  FROM {{ source('src_system_billing', 'list_prices') }}
)
SELECT * FROM stg_databricks_prices
