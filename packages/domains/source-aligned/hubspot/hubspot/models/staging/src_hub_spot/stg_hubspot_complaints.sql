SELECT
  id AS hubspot_complaints_id
  -- extracts hubspot contact, deal and ticket ids from the json column
  , {{ extract_associated_ids('associate_json', ['contacts', 'deals', 'tickets']) }}
  , {{ nullify_empty_strings('product', 'lct_product') }}
  , {{ nullify_empty_strings('cx_category') }}
  , {{ nullify_empty_strings('cx_subcategory') }}
  , {{ nullify_empty_strings('type') }}
  , {{ nullify_empty_strings('escalation_reason') }}
  , {{ nullify_empty_strings('install_region') }}
  , {{ nullify_empty_strings('install_subregion') }}
  , {{ nullify_empty_strings('internally_raised_by__name_', 'internally_raised_by_name') }}
  , {{ nullify_empty_strings('internally_raised_by__team_', 'internally_raised_by_team') }}
  , days_open::BIGINT
  , hs_pipeline_stage::BIGINT
  , {{ localize('hs_createdate') }} AS created_at
  , {{ localize('hs_lastmodifieddate') }} AS updated_at
  , {{ localize('date_closed') }} AS closed_at
FROM {{ source('src_hub_spot', 'hubspot_complaints') }}
