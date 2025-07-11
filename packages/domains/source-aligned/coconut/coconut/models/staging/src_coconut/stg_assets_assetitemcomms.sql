SELECT
  id AS asset_comms_id
  , firmware_version
  , manufacturer_month
  , manufacturer_year
  , chf_id AS comms_hub_id
  , gpf_id AS gas_proxy_id
  , order_id
  , pallet_id
  , is_active
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'assets_assetitemcomms') }}
