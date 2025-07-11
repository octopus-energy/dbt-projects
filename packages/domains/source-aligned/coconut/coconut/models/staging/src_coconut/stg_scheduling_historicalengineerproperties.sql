SELECT
  history_id
  , user_id
  , history_user_id
  , {{ nullify_empty_strings('custom_status', 'engineer_status') }}
  , booking_days_ahead AS time_shield
  , BOOLEAN(is_manually_scheduled) AS is_manually_scheduled
  , BOOLEAN(has_scheduling_enabled) AS has_scheduling_enabled
  , DATE(active_until_date) AS active_until_date
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'scheduling_historicalengineerproperties') }}
