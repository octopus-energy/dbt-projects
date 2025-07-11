SELECT
  id AS asset_type_id
  , created_at
  , updated_at
  , display_type
  , sku
  , manufacturer
  , meter_asset_provider
  , item_class
  , is_serialized
  , is_active
  , stock_id
  , bybox_supplier
  , supplier
  , _original_id AS original_id
  , _input_filename AS input_filename
  , _ingestion_timestamp AS ingestion_timestamp
FROM {{ source('src_coconut', 'assets_assettype') }}
