WITH dim_information_schema_tables AS (
  SELECT
    information_schema_tables_id
    ,table_catalog
    ,table_schema
    ,table_name
    ,table_type
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
  FROM {{ ref('stg_information_schema_tables') }}
)
SELECT * FROM dim_information_schema_tables