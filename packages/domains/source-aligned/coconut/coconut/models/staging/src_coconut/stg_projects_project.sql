SELECT
  id AS project_id
  , external_id
  , customer_id
  , property_id
  , kraken_quote_id
  , hubspot_contact_id
  , CAST(CASE WHEN LOWER(is_active) = 'true' THEN 1 ELSE 0 END AS BOOLEAN) AS is_active
  , CAST(CASE WHEN LOWER(is_training) = 'true' THEN 1 ELSE 0 END AS BOOLEAN) AS is_training
  , type
  , status
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'projects_project') }}
