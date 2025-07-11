SELECT
  CAST(id AS BIGINT) AS asset_event_id
  , CAST(event_type AS STRING) AS event_type
  , CAST(data AS STRING) AS metadata
  , CAST(process_id AS STRING) AS asset_meter_id
  , CAST(_original_id AS BIGINT) AS original_id
  , TO_UTC_TIMESTAMP(_timestamp, 'UTC') AS updated_at
  , TO_UTC_TIMESTAMP(occurred_at, 'UTC') AS occurred_at
FROM {{ source('src_coconut', 'assets_assetitemmeterevent') }}
