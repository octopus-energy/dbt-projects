SELECT
  id AS appointment_event_id
  , appointment_id
  , actionevent_id AS action_event_id
FROM {{ source('src_coconut', 'work_orders_appointment_action_events') }}
