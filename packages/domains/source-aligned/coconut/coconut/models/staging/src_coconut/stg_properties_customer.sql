SELECT
  {{ hash_sensitive_columns('stg_properties_customer_pii') }}
FROM {{ ref('stg_properties_customer_pii') }}
