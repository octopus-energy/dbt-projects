SELECT
  id AS bybox_event_id
  , event_type
  , data AS metadata
  , process_id
  , TO_UTC_TIMESTAMP(occurred_at, 'UTC') AS occurred_at
FROM {{ source('src_coconut', 'gbr_asset_management_assetbyboxevent') }}
