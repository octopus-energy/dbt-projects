SELECT
  id AS property_id
  , pid
  , coordinates_id
  , postcode
  , address  --PII
  , exists_in_kraken
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'properties_property') }}
