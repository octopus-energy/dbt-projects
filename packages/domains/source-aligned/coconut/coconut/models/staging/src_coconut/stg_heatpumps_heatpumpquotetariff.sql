SELECT
  id AS heat_pump_quotetariff_id
  , created_by_id
  , code
  , current
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'heatpumps_heatpumpquotetariff') }}
