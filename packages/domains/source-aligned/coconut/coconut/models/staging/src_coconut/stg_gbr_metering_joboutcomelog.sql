SELECT
  id AS job_outcome_id
  , client
  , task
  , serial_number
  , appointment_id
  , existing_meterpoint_data_id
  , status AS job_outcome_status
  , _original_id AS original_id
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'gbr_metering_joboutcomelog') }}
