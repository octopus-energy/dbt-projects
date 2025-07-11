SELECT
  id AS work_order_id
  , project_id
  , external_id
  , template_code
  , notes
  , status
  , CAST(CASE WHEN LOWER(deleted) = 'true' THEN 1 ELSE 0 END AS boolean) AS deleted
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'work_orders_workorder') }}
