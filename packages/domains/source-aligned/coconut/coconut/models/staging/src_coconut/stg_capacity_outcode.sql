SELECT
  id AS outcode_id
  , patch_id
  , outcode
  , latitude
  , longitude
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'capacity_outcode') }}
