WITH fct_databricks_usage AS (
  SELECT
    -- noqa: disable=RF03
    record_id
    , account_id
    , workspace_id
    , sku_name
    , usage_start_time
    , usage_end_time
    , usage_date
    , usage_quantity
    , usage_unit
    , cloud
    , custom_tags
    , usage_metadata
    --- Tags from our pyspark processes
    , ELEMENT_AT(custom_tags, "job/dag_id") AS tag_job_dag_id
    , ELEMENT_AT(custom_tags, "job/installation") AS tag_job_installation
    , ELEMENT_AT(custom_tags, "job/environment") AS  tag_job_environment
    , ELEMENT_AT(custom_tags, "job/template") AS  tag_job_template
    , ELEMENT_AT(custom_tags, "job/ingestion_command") AS tag_job_ingestion_command
    --- Tags for tracking aws costs
    , ELEMENT_AT(custom_tags, "user:ResourceSource") AS tag_aws_user_resource_source
    , ELEMENT_AT(custom_tags, "user:ResourceId") AS tag_aws_user_resource_id
    , ELEMENT_AT(custom_tags, "user:ResourceType") AS tag_aws_user_resource_type
    --- Tags for ktl product/service
    , ELEMENT_AT(custom_tags, "ktl:product") AS tag_ktl_product
    , ELEMENT_AT(custom_tags, "ktl:product-group") AS tag_ktl_product_group
    , ELEMENT_AT(custom_tags, "ktl:service") AS tag_ktl_service
    --- new columns from usage_metadata
    , usage_metadata.cluster_id AS usage_metadata_cluster_id
    , usage_metadata.job_id AS usage_metadata_job_id
    , usage_metadata.warehouse_id AS usage_metadata_warehouse_id
    , usage_metadata.instance_pool_id AS usage_metadata_instance_pool_id
    , usage_metadata.node_type AS usage_metadata_node_type
    -- noqa: enable=RF03
  FROM {{ ref('stg_databricks_usage') }}
)
SELECT * FROM fct_databricks_usage
