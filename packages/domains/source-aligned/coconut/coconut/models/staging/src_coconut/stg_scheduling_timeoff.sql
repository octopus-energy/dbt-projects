SELECT
  id AS timeoff_id
  , user_id
  , type
  , {{ localize(split_str_time_range("leave_period")) }} AS leave_from
  , {{ localize(split_str_time_range("leave_period", False)) }} AS leave_to
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'scheduling_timeoff') }}
