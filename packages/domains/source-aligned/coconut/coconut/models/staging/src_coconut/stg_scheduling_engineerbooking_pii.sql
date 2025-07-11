SELECT
  id AS engineer_booking_id
  , user_id
  , start_location_id
  , work_preference_id
  , activities
  , appointments
  , am_capacity
  , pm_capacity
  , total_capacity
  , total_free_minutes
  , total_driving_minutes
  , total_appointment_minutes
  , total_break_minutes
  , total_timeoff_minutes
  , is_valid
  , date AS engineer_booking_date
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'scheduling_engineerbooking') }}
