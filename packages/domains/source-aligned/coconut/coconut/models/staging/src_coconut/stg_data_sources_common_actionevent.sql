SELECT
  id AS action_event_id
  , triggered_by_id AS triggered_by_user_id
  , metadata AS event_metadata
  , type AS event_type
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'data_sources_common_actionevent') }}
