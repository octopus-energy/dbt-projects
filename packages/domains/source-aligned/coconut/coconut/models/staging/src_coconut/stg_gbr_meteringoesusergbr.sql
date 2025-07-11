SELECT
  id AS metering_oes_id
  , user_id
  , bybox_engineer_id
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'gbr_metering_oesusergbr') }}
