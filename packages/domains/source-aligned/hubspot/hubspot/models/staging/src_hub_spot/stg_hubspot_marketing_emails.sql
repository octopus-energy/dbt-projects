SELECT
  {{ hash_sensitive_columns('stg_hubspot_marketing_emails_pii') }}
FROM {{ ref('stg_hubspot_marketing_emails_pii') }}
