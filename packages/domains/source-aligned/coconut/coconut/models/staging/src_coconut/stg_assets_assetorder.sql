SELECT
  id AS asset_order_id
  , created_at
  , updated_at
  , delivery_number
  , purchase_order_number
  , order_date
  , quantity
  , asset_type_id
  , created_by_id
  , status
  , external_id
  , is_active
  , _original_id AS original_id
  , _input_filename AS input_filename
  , _ingestion_timestamp AS ingestion_timestamp
FROM {{ source('src_coconut', 'assets_assetorder') }}
