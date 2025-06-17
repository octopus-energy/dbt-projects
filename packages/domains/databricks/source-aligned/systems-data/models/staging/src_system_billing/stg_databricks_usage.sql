WITH stg_databricks_usage AS (
  SELECT
    record_id 
    , account_id
    , workspace_id
    , sku_name
    , usage_start_time
    , usage_end_time
    , usage_date
    , CAST(usage_quantity AS DOUBLE) AS usage_quantity
    , usage_unit
    , cloud
    , custom_tags 
    , usage_metadata

  FROM {{ source('src_system_billing', 'usage') }}
  ---At the workspace we filter the data from the system table for just the
  -- current workspace as for OEXX clients this data is stored at the
  -- metastore level and few clients might share the table
  WHERE workspace_id = {{ env_var('DATABRICKS_WORKSPACE_ID') }}
    -- if a test workspace id is supplied, include that too
    OR workspace_id = {{ env_var('TEST_DATABRICKS_WORKSPACE_ID', 0) }}
)
SELECT * FROM stg_databricks_usage
