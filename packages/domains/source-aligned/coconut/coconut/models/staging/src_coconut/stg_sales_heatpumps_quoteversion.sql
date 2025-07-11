SELECT
  id AS quote_version_id
  , CAST(version_id AS BIGINT) AS version_id
  , extra_costs_id
  , inclusions_exclusions_id
  , performance_data_id
  , tariff_snapshot_id AS heat_pump_quotetariff_id
  , heatloss_calculation_id
  , CAST(quote_id AS BIGINT) AS quote_id
  , CAST(estimated_materials_cost AS BIGINT) AS estimated_materials_cost
  , radiator_schedule
  , CAST(current_rad_count AS BIGINT) AS current_rad_count
  , CAST(rad_to_install_count AS BIGINT) AS rad_to_install_count
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'sales_heatpumps_quoteversion') }}
