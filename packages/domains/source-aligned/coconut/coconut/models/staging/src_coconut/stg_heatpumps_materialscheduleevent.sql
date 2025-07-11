SELECT
  id AS schedule_event_id
  , process_id AS schedule_id
  , event_type
  , data
  , TO_UTC_TIMESTAMP(occurred_at, 'UTC') AS occurred_at
FROM {{ source('src_coconut', 'heatpumps_materialscheduleevent') }}
