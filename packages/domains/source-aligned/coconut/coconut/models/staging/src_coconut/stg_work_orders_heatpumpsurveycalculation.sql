SELECT
  id AS heatloss_calculation_id
  , property_plan_id
  , work_order_id
  , property_image_id
  , task_id
  , triggered_by_id
  , calculation
  , errors
  , process_status
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'work_orders_heatpumpsurveycalculation') }}
