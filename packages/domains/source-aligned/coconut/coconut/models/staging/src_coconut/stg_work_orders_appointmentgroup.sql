SELECT
  id AS appointment_group_id
  , requirement_id
  , work_order_id
  , appointments
  , skill_codes
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'work_orders_appointmentgroup') }}
