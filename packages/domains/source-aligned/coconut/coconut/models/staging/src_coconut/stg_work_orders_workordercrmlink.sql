SELECT
  id AS work_order_crm_link_id
  , {{ nullify_empty_strings('workorder_id') }}
  , {{ nullify_empty_strings('crmlink_id') }}
FROM {{ source('src_coconut', 'work_orders_workorder_crm_links') }}
