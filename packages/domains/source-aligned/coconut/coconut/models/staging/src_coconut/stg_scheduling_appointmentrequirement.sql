SELECT
  id AS appointment_requirement_id
  , days
  , skill_codes
  , duration_minutes
  , requirement_name
  , assigned_role
  , apppointment_matches_calculation_id AS appointment_matches_calculation_id
  , work_order_id
  , work_order_requirement_id
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'scheduling_appointmentrequirement') }}
