SELECT
  id AS asset_install_id
  , appointment_id
  , comms_id AS comms_hub_id
  , esme_id
  , gsme_id
  , ihdppmid_id AS in_home_display_id
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at

FROM {{ source('src_coconut', 'assets_assetiteminstallation') }}
