SELECT
  {{ hash_sensitive_columns('stg_hubspot_tickets_pii') }}
FROM {{ ref('stg_hubspot_tickets_pii') }}
