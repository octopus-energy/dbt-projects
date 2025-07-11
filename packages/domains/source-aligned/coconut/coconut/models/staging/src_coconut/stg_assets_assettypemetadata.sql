SELECT
  id AS asset_metadata_id
  , asset_type_id
  , `_original_id` AS original_id
  , name AS metadata_name
  , value AS metadata_value
  , value_type AS metadata_value_type
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'assets_assettypemetadata') }}
