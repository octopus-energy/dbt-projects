SELECT
  id AS line_item_id
  , warehouse_item_id
  , schedule_id
  , price_minor
  , quantity
  , pick_status
  , is_active
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
  , TO_UTC_TIMESTAMP(deleted_at, 'UTC') AS deleted_at
FROM {{ source('src_coconut', 'heatpumps_lineitem') }}
