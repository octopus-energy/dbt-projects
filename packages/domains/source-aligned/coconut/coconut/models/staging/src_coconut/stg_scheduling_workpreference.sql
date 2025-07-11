SELECT
  id AS work_preference_id
  , user_id
  , crew_id
  , break_hours
  , only_on_days
  , job_duration_multiplier
  , skill_codes
  , location
  , postcode
  , max_driving_minutes
  , active_period
  , {{ ms_int_to_time_column('works_from') }} AS works_from
  , {{ ms_int_to_time_column('works_until') }} AS works_until
  , {{ to_date(split_str_time_range("active_period")) }} AS active_from
  , {{ to_date(split_str_time_range("active_period", False)) }} AS active_to
  , {{ to_date('date_from') }} AS date_from
  , {{ to_date('date_until') }} AS date_to
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'scheduling_workpreference') }}
