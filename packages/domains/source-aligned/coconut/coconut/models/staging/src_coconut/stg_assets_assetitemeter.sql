SELECT
  id AS asset_meter_id
  , serial_number
  , manufacturer_month
  , manufacturer_year
  , firmware_version
  , certification_date
  , certification_expiry_date
  , install_code_id
  , order_id
  , pallet_id
  , device_manufacturer
  , device_model
  , device_type
  , is_registered
  , smets_chts_version
  , is_active
  , commission_status
  , mpxn
  , property_id
  , _original_id AS original_id
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
  , TO_UTC_TIMESTAMP(deleted_at, 'UTC') AS deleted_at
FROM {{ source('src_coconut', 'assets_assetitemmeter') }}
