SELECT
  id AS work_order_regulatory_documentation_id
  , work_order_id
  , file_id
  , process_status
  , documentation_type
  , errors
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'work_orders_workorderregulatorydocumentation') }}
