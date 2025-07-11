SELECT
  id AS schedule_id
  , project_id
  , is_draft
  , is_active
  , process_status
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'heatpumps_materialschedule') }}
