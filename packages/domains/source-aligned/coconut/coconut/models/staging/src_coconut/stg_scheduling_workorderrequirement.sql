SELECT
  id AS work_order_requirement_id
  , work_order_id
  , additional_skill_codes
  , work_order_type
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'scheduling_workorderrequirement') }}
