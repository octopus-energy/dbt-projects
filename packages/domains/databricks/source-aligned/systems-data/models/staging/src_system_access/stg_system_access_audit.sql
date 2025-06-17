WITH stg_system_access_audit AS (
  SELECT
    -- since this is copying system tables, no need for surrogate key
    MONOTONICALLY_INCREASING_ID() AS system_access_audit_id
    ,account_id
    ,workspace_id
    ,version
    ,event_time
    ,event_date
    ,source_ip_address
    ,user_agent
    ,session_id
    ,user_identity
    ,service_name
    ,action_name
    ,request_id
    ,request_params
    ,response
    ,audit_level
    ,event_id
  FROM {{ source('src_databricks_system_access', 'audit') }}
)
SELECT * FROM stg_system_access_audit


