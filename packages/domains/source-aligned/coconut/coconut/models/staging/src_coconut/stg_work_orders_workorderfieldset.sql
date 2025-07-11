SELECT
  CAST(id AS STRING) AS work_order_fieldset_id
  , {{ nullify_empty_strings('work_order_id') }}
  , {{ nullify_empty_strings('template_code') }}
  , {{ nullify_empty_strings('status') }}
  , CAST(is_deleted AS BOOLEAN) AS is_deleted
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'work_orders_workorderfieldset') }}
