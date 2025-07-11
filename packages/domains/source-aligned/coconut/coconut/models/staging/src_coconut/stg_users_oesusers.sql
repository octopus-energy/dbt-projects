SELECT
  {{ hash_sensitive_columns('stg_users_oesusers_pii') }}
FROM {{ ref('stg_users_oesusers_pii') }}
