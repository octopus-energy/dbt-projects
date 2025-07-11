SELECT
  {{ hash_sensitive_columns('stg_scheduling_engineerbooking_pii') }}
FROM {{ ref('stg_scheduling_engineerbooking_pii') }}
