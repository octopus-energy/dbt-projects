SELECT
  id AS appointment_cancellation_id
  , appointment_id
  , cancelled_by_id
  , field_user_id
  , customer_signature_id
  , technical_code
  , cancellation_type
  , reason AS cancellation_reason
  , notes AS cancellation_notes
  , tags
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'work_orders_appointmentcancellation') }}
