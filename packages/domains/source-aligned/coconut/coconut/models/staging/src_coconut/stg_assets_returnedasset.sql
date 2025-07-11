SELECT
  id AS returned_asset_id
  , asset_id
  , work_order_id
  , return_barcode
  , return_reason
  , evidence
  , comments
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'assets_returnedasset') }}
