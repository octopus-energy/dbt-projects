WITH dim_system_information_schema_catalog_provider_share_usage AS (
    SELECT 
        information_schema_catalog_provider_share_usage_id
        , catalog_name
        , provider_name
        , share_name
    FROM {{ ref('stg_system_information_schema_catalog_provider_share_usage') }}
)
SELECT * FROM dim_system_information_schema_catalog_provider_share_usage
