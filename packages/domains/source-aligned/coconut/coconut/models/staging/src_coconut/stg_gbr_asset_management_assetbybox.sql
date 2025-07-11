SELECT
  id AS bybox_asset_id
  , asset_type
  , status AS bybox_status
  , tote_tracking_number
  , comms_asset_id
  , engineer_id
  , ihdppmid_asset_id
  , meter_asset_id
  , regulator_asset_id
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'gbr_asset_management_assetbybox') }}
