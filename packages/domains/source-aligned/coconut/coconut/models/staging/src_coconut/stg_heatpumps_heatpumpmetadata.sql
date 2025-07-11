SELECT
  id AS heat_pump_meta_id
  , warehouse_item_id
  , performance
  , sound_power
  , design_output
  , width
  , height
  , depth
  , clearance_front
  , clearance_rear
  , ena_reg_no
  , max_demand
  , dhw_flow
  , dhw_residual_head
  , htg_residual_head
  , heating_scop
  , mcs_certification_number
  , has_blygold_coating
FROM {{ source('src_coconut', 'heatpumps_heatpumpmetadata') }}
