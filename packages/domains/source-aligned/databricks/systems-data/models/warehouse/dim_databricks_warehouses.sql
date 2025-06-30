WITH dim_databricks_warehouses AS (
  SELECT 
    warehouse_id
    , workspace_id
    , warehouse_name
  FROM {{ ref('stg_databricks_warehouses') }}
)
SELECT * FROM dim_databricks_warehouses
