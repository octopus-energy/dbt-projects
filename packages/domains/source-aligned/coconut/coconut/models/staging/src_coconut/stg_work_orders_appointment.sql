SELECT
  id AS appointment_id
  , kraken_siteworks_id
  , kraken_appointment_id
  , field_user_id
  , work_order_id
  , requirement_id
  , external_id
  , created_by_user_id
  , type
  , status
  , notes
  , external_notes
  , metadata
  , timeslot
  , CAST(CASE WHEN LOWER(deleted) = 'true' THEN 1 ELSE 0 END AS BOOLEAN) AS deleted
  , CAST(CASE WHEN LOWER(is_training) = 'true' THEN 1 ELSE 0 END AS BOOLEAN) AS is_training
  , assigned_role
  , duration_in_minutes
  , is_in_jeopardy
  , is_engineer_locked
  , DATE(date) AS appointment_date
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'work_orders_appointment') }}
