SELECT
  id AS cylinder_meta_id
  , height
  , diameter
  , volume
FROM {{ source('src_coconut', 'heatpumps_cylindermetadata') }}
