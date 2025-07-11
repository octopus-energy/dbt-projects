SELECT
  id AS other_costs_id
  , boiler_upgrade_scheme_minor
  , labour_day_cost
  , labour_days
  , waste_collection_minor
  , additional_services_minor
  , vat
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
FROM {{ source('src_coconut', 'sales_heatpumps_othercostssnapshot') }}
