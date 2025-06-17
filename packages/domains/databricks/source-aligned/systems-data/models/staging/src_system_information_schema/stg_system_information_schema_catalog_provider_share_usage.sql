WITH stg_system_information_schema_catalog_provider_share_usage AS (
SELECT 
    CONCAT('catalog_name', 'provider_name', 'share_name') AS information_schema_catalog_provider_share_usage_id
    , catalog_name
    , provider_name
    , share_name
FROM {{ source('src_system_information_schema', 'catalog_provider_share_usage') }}
)
SELECT * FROM stg_system_information_schema_catalog_provider_share_usage
