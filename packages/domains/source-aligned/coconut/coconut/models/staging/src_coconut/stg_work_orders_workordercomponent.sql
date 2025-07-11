SELECT
  id AS work_order_component_id
  , fieldset_id AS work_order_fieldset_id
  , type
  , boolean
  , NULLIF(REPLACE(REPLACE(selection, '{', ''), '}', ''), '') AS selection
  , text
  , integer
  , float
  , list
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'work_orders_workordercomponent') }}
