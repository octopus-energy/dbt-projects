SELECT
  {{ hash_sensitive_columns('stg_hubspot_contacts_pii') }}
FROM {{ ref('stg_hubspot_contacts_pii') }}
