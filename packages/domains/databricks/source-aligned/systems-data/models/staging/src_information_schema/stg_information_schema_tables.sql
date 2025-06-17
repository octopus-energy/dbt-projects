WITH stg_information_schema_tables AS (
  SELECT
    -- since this is copying system tables, no need for PK tests
    CONCAT(table_catalog, '.', table_schema, '.', table_name) AS information_schema_tables_id
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
  FROM {{ source('src_information_schema', 'tables') }}
)
SELECT * FROM stg_information_schema_tables


