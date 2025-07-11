SELECT
  {{ dbt_utils.generate_surrogate_key(['id', 'value', 'timestamp']) }} AS ticket_owner_history_id
  , id AS hubspot_ticket_id
  , CAST(value AS STRING) AS ticket_owner_id
  , TO_UTC_TIMESTAMP(timestamp, 'UTC') AS ticket_owner_updated_at
FROM {{ source('src_hub_spot', 'hubspot_ticket_owner_history') }}
