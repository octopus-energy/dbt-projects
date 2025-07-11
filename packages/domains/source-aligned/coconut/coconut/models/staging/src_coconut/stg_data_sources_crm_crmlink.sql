SELECT
  id AS crmlink_id
  , link_type
  , link_source
  , value
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'data_sources_crm_crmlink') }}
