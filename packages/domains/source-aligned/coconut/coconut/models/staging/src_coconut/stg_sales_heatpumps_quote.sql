SELECT
  id AS quote_id
  , material_schedule_id AS schedule_id
  , project_id
  , external_id
  , is_active
  , is_current
  , process_status
  , `type` AS hp_quote_type
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'sales_heatpumps_quote') }}
