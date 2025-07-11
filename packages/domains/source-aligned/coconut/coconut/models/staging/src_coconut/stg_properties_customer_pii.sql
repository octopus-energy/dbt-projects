SELECT
  id AS customer_id
  , account_number
  , given_name    -- PII
  , family_name    -- PII
  , phone    -- PII
  , proxy_extension
  , email    -- PII
  , exists_in_kraken
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'properties_customer') }}
