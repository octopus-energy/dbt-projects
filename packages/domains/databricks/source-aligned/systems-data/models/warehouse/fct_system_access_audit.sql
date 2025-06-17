WITH fct_system_access_audit AS (
  SELECT
    -- since this is copying system tables, no need for surrogate key
    system_access_audit_id
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
  FROM {{ ref('stg_system_access_audit') }}
)
SELECT * FROM fct_system_access_audit