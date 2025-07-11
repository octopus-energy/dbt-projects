SELECT
  id AS installer_application_id
  , application_id
  , work_order_id
  , status
FROM {{ source('src_coconut', 'gbr_work_orders_installerapplication') }}
