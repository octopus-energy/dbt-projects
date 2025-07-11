SELECT
  id AS appointment_scheduling_id
  , appointment_id
  , user_id
  , assigned_timeslot
  , location
  , TO_UTC_TIMESTAMP(estimated_start_at, 'UTC') AS estimated_start_at
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'scheduling_appointmentscheduling') }}
