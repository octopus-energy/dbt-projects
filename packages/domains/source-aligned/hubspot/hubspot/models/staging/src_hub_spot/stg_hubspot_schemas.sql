WITH schemas AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['object_type', 'property_name']) }}
    AS hubspot_schema_property_id
    , {{ nullify_empty_strings('object_type') }}
    , {{ nullify_empty_strings('label_plural') }}
    , {{ nullify_empty_strings('object_description') }}
    , CAST(object_archived AS BOOLEAN) AS is_object_archived
    , TO_UTC_TIMESTAMP(object_created_at, 'UTC') AS object_created_at
    , TO_UTC_TIMESTAMP(object_updated_at, 'UTC') AS object_updated_at
    , {{ nullify_empty_strings('property_name') }}
    , {{ nullify_empty_strings('property_label') }}
    , {{ nullify_empty_strings('property_type') }}
    , {{ nullify_empty_strings('property_description') }}
    , {{ nullify_empty_strings('group_name') }}
    , CAST(created_user_id AS BIGINT) AS created_user_id
    , CAST(updated_user_id AS BIGINT) AS updated_user_id
    , CAST(calculated AS BOOLEAN) AS is_calculated
    , CAST(property_archived AS BOOLEAN) AS is_property_archived
    , CAST(`hidden` AS BOOLEAN) AS is_hidden
    , TO_UTC_TIMESTAMP(property_created_at, 'UTC') AS property_created_at
    , TO_UTC_TIMESTAMP(property_updated_at, 'UTC') AS property_updated_at
  FROM {{ source('src_hub_spot', 'hubspot_schemas') }}
)

SELECT
  schemas.hubspot_schema_property_id
  , schemas.created_user_id
  , schemas.updated_user_id
  , schemas.object_type
  , schemas.label_plural
  , schemas.object_description
  , schemas.is_object_archived
  , schemas.property_name
  , schemas.property_label
  , schemas.property_type
  , schemas.property_description
  , schemas.group_name
  , schemas.is_calculated
  , schemas.is_property_archived
  , schemas.is_hidden
  , schemas.property_created_at
  , schemas.property_updated_at
  , schemas.object_created_at
  , schemas.object_updated_at
FROM schemas
