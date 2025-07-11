SELECT
  {{ hash_sensitive_columns('stg_hibob_employee_latest_pii') }}
FROM {{ ref('stg_hibob_employee_latest_pii') }}
