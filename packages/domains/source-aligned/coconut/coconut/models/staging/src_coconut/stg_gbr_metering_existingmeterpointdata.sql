SELECT
  id AS existingmeterpointdata_id
  , property_id
  , energy AS fuel_type
  , mpxn
  , supplier
  , serial_numbers
  , ssc
  , related_mpxn
  , is_export
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'gbr_metering_existingmeterpointdata') }}
