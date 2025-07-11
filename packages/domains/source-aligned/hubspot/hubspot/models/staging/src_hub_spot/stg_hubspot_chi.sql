SELECT
  chi.id AS hubspot_chi_id
  -- extracts hubspot contact, deal and ticket ids from the json column
  , {{ extract_associated_ids('chi.associate_json', ['contacts', 'deals', 'tickets']) }}
  , (chi.chi_score)::INT AS chi_score
  , {{ nullify_empty_strings('customer_service') }}
  , (chi.customer_service__score_)::INT AS customer_service_score
  , {{ nullify_empty_strings('efficiency') }}
  , (chi.efficiency__score_)::INT AS efficiency_score
  , {{ nullify_empty_strings('informative') }}
  , (chi.informative__score_)::INT AS informative_score
  , {{ nullify_empty_strings('overall_value_for_money') }}
  , (chi.overall_value_for_money__score_)::INT AS overall_value_for_money_score
  , DATE(chi.survey_submission_date) AS survey_submission_date
  , {{ nullify_empty_strings('product', 'lct_product') }}
  , {{ nullify_empty_strings('survey_type') }} 
  , {{ nullify_empty_strings('sentiment') }}
  , TO_UTC_TIMESTAMP(chi.hs_createdate, 'UTC') AS created_at
FROM {{ source('src_hub_spot', 'hubspot_chi') }} AS chi
