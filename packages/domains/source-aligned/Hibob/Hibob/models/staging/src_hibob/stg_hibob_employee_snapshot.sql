SELECT
  {{ hash_sensitive_columns('stg_hibob_employee_snapshot_pii') }}
FROM {{ ref('stg_hibob_employee_snapshot_pii') }}
