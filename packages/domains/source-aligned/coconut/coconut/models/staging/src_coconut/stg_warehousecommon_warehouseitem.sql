SELECT
  id AS warehouse_item_id
  , is_active
  , category_primary
  , category_secondary
  , supplier
  , manufacturer_code
  , manufacturer_description
  , price_minor
  , internal_code
  , internal_description
  , in_warehouse
  , location_code
  , weight_grams
  , current_stock_level
  , maximum_stock_level
  , minimum_stock_level
  , project_type
  , van_stock
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'warehousecommon_warehouseitem') }}
