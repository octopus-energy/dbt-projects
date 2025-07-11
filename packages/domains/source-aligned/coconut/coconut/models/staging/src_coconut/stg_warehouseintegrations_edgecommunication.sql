SELECT
  id AS warehouse_integration_id
  , material_schedule_id
  , triggered_by_id AS triggered_by_user_id
  , depot_id
  , depot_name
  , type AS warehouse_integration_api_call_type
  , call_status AS warehouse_integration_api_call_status
  , request_payload
  , response_payload
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'warehouseintegrations_edgecommunication') }}
