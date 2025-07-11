SELECT
  id AS performance_data_id
  , hot_water_sterilization_kwh
  , hot_water_electricity_kwh
  , heating_kwh
  , current_dhw_cost
  , current_heating_cost
  , current_total_cost
  , clever_dhw_cost
  , clever_heating_cost
  , clever_total_cost
  , clever_saving_percent
  , flow_temperature
  , tariff_type
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'sales_heatpumps_performancedatasnapshot') }}
