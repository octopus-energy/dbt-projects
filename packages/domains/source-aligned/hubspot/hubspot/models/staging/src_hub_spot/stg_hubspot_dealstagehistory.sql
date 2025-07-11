SELECT
  {{ dbt_utils.generate_surrogate_key(['id', 'value', 'timestamp']) }} AS deal_stage_history_id
  , id AS hubspot_deal_id
  , CAST(value AS STRING) AS deal_stage
  , TO_UTC_TIMESTAMP(timestamp, 'UTC') AS deal_updated_at
FROM {{ source('src_hub_spot', 'hubspot_deal_stage_history') }}
