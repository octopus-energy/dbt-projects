SELECT
  polygon_ptr_id AS polygon_id
  , contour
  , centroid
FROM {{ source('src_coconut', 'capacity_isochrone') }}
