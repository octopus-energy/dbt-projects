SELECT
  id AS asset_install_code_id
  , device_id
  , install_code
  , `_original_id` AS original_id
  , CAST(is_active AS BOOLEAN) AS is_active
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'assets_assetinstallcode') }}
