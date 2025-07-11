SELECT
  id AS coordinates_id
  , longitude
  , latitude
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'properties_coordinates') }}
