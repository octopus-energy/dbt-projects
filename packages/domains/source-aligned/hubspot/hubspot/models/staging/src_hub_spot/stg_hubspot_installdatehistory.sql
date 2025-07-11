SELECT
  {{ dbt_utils.generate_surrogate_key(
        ['id', 'value', 'timestamp']) }} AS ticket_install_date_history_id
  , id AS hubspot_ticket_id
  , {{ to_date('value') }}  AS ticket_install_date
  , TO_UTC_TIMESTAMP(timestamp, 'UTC') AS ticket_install_date_updated_at
FROM {{ source('src_hub_spot', 'hubspot_ticket_install_date_history') }}
