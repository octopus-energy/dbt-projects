WITH dim_system_information_schema_tables AS (
  SELECT
    information_schema_tables_id
    ,table_catalog
    ,table_schema
    ,table_name
    ,{{ safe_cast('table_type', 'string') }} as table_type
    ,is_insertable_into
    ,commit_action
    ,table_owner
    ,comment
    ,created
    ,created_by
    ,last_altered
    ,last_altered_by
    ,data_source_format
    ,storage_sub_directory
  FROM {{ ref('stg_system_information_schema_tables') }}
)
SELECT * FROM dim_system_information_schema_tables
