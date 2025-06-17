WITH stg_databricks_warehouses AS (
  SELECT 
    warehouse_id
    , workspace_id
    , warehouse_name
  FROM {{ ref('data_databricks_warehouses') }}
)
SELECT * FROM stg_databricks_warehouses
