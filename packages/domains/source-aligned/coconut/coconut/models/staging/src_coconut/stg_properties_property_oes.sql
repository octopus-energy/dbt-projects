SELECT
  {{ hash_sensitive_columns('stg_properties_property_oes_pii') }}
FROM {{ ref('stg_properties_property_oes_pii') }}
