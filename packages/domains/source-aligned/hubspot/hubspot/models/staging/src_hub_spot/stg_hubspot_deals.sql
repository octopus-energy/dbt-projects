SELECT
  {{ hash_sensitive_columns('stg_hubspot_deals_pii') }}
FROM {{ ref('stg_hubspot_deals_pii') }}