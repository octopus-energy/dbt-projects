SELECT
  id AS radiator_meta_id
  , warehouse_item_id
  , width
  , height
  , n_coefficient
  , watts
FROM {{ source('src_coconut', 'heatpumps_radiatormetadata') }}
