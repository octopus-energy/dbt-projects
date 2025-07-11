SELECT
  id AS hubspot_enquiry_id
  -- extracts hubspot contact, deal and ticket ids from the json column
  , {{ extract_associated_ids('associate_json', ['contacts', 'deals', 'tickets']) }}
  , CASE
    WHEN product = 'heat_pump' THEN 'HEATPUMP'
    WHEN product = 'solar_battery' THEN 'SOLAR'
    WHEN product IN ('ev_charger', 'ev_charger;solar_battery') THEN 'EV'
  END AS lct_product
  , IFF(product = 'ev_charger;solar_battery', TRUE, FALSE) AS is_ev_charger_and_solar_battery
  , {{ nullify_empty_strings('enquiry_source') }}
  , {{ nullify_empty_strings('enquiry_id') }}
  , {{ nullify_empty_strings('addressable_lead') }}
  , {{ nullify_empty_strings('house_type') }}
  , uprn::BIGINT
  , {{ nullify_empty_strings('lct_own_or_rent') }}
  , {{ nullify_empty_strings('partnership') }}
  , {{ nullify_empty_strings('third_party_lead_channel') }}
  
  -- Heat pump specific fields
  , {{ nullify_empty_strings('hp__current_heating_system', 'hp_current_heating_system') }}
  , {{ nullify_empty_strings('hp__non_addressable_reason', 'hp_non_addressable_reason') }}
  , {{ nullify_empty_strings('hp__space_for_cylinder', 'hp_space_for_cylinder') }}
  , {{ nullify_empty_strings('hp__space_for_heat_pump', 'hp_space_for_heat_pump') }}
  , {{ nullify_empty_strings('hp__renovations_in_progress', 'hp_renovations_in_progress') }}
  , hp__heat_loss_estimate::DOUBLE AS hp_heat_loss_estimate
  , hp__indicative_price__high_temp_::DOUBLE AS hp_indicative_price_high_temp
  , hp__indicative_price__low_temp_::DOUBLE AS hp_indicative_price_low_temp
  , hp__online_quote_price::DOUBLE AS hp_online_quote_price
  , {{ nullify_empty_strings('hp__quote_code', 'hp_quote_code') }}
  , {{ localize('hp__renovations_completion_date') }} AS hp_renovations_completion_date
  
  -- Solar specific fields
  , {{ nullify_empty_strings('slr__additional_information', 'slr_additional_information') }}
  , {{ nullify_empty_strings('slr__battery_requested', 'slr_battery_requested') }}
  , {{ nullify_empty_strings('slr__journey_pathway', 'slr_journey_pathway') }}
  , {{ nullify_empty_strings('slr__non_addressable_reasons', 'slr_non_addressable_reasons') }}
  , {{ nullify_empty_strings('slr__pitched___flat_roof', 'slr_pitched_flat_roof') }}
  , {{ nullify_empty_strings('slr__requested_ev_charger', 'slr_requested_ev_charger') }}
  , {{ nullify_empty_strings('slr__roof_tile_type', 'slr_roof_tile_type') }}
  , {{ nullify_empty_strings('slr__partnership', 'slr_partnership') }}
  , slr__number_of_panels::DOUBLE AS slr_number_of_panels
  
  -- LCT fields
  , lct__estimated_annual_electricity_usage::DOUBLE AS lct_estimated_annual_electricity_usage
  , {{ nullify_empty_strings('lct__in_service_area', 'lct_in_service_area') }}
  , {{ nullify_empty_strings('lct__latest_utm_campaign', 'lct_latest_utm_campaign') }}
  , {{ nullify_empty_strings('lct__latest_utm_content', 'lct_latest_utm_content') }}
  , {{ nullify_empty_strings('lct__latest_utm_medium', 'lct_latest_utm_medium') }}
  , {{ nullify_empty_strings('lct__latest_utm_source', 'lct_latest_utm_source') }}
  , {{ nullify_empty_strings('lct__online_journey_pathway', 'lct_online_journey_pathway') }}
  , {{ nullify_empty_strings('lct_kraken_account_id') }}
  
  -- Marketing/partner fields
  , {{ nullify_empty_strings('mvf_lead_id') }}
  , {{ nullify_empty_strings('mvf_distribution_id') }}
  , {{ nullify_empty_strings('otp_partner__large_', 'otp_partner_large') }}
  , {{ nullify_empty_strings('otp_partner__medium_', 'otp_partner_medium') }}
  , {{ nullify_empty_strings('otp_partner__small_', 'otp_partner_small') }}
  
  -- HubSpot system fields
  , hs_object_id::BIGINT
  , {{ nullify_empty_strings('hs_tag_ids') }}
  , {{ nullify_empty_strings('hubspot_owner_id') }}
  , {{ nullify_empty_strings('hubspot_team_id') }}
  , {{ nullify_empty_strings('hs_object_source_detail_1') }}
  
  -- Timestamps
  , {{ localize('hs_createdate') }} AS created_at
  , {{ localize('hs_lastmodifieddate') }} AS updated_at
  , {{ localize('hubspot_owner_assigneddate') }} AS hubspot_owner_assigned_at
  
FROM {{ source('src_hub_spot', 'hubspot_enquiries') }}
