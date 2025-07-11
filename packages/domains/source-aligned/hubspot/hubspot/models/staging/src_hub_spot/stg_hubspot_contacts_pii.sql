SELECT
  CAST(id AS BIGINT) AS hubspot_contact_id
  , {{ nullify_empty_strings('address') }}
  , {{ nullify_empty_strings('ashp___ev_purchase_planned_','ashp_ev_purchase_planned') }}
  , {{ nullify_empty_strings('buying_window') }}
  , {{ nullify_empty_strings('city') }}
  , {{ nullify_empty_strings('country') }}
  , {{ nullify_empty_strings('email') }}
  , {{ nullify_empty_strings('epc_review__sales_team_','epc_review_sales_team') }}
  , {{ nullify_empty_strings('estimated_consumption') }}
  , {{ nullify_empty_strings('fax') }}
  , {{ nullify_empty_strings('first_conversion_event_name') }}
  , {{ nullify_empty_strings('firstname') }}
  , {{ nullify_empty_strings('has_water_tank_','has_water_tank') }}
  , {{ nullify_empty_strings('heating_fuel_source') }}
  , {{ nullify_empty_strings('house_type') }}
  , {{ nullify_empty_strings('insulation_in_property') }}
  , {{ nullify_empty_strings('jobtitle','job_title') }}
  , {{ nullify_empty_strings('kraken_account_id') }}
  , {{ nullify_empty_strings('kraken_enquiry_id') }}
  , {{ nullify_empty_strings('lastname') }}
  , {{ nullify_empty_strings('onboarding_survey_label') }}
  , {{ nullify_empty_strings('phone') }}
  , {{ nullify_empty_strings('lifecyclestage','life_cycle_stage') }}
  , {{ nullify_empty_strings('message') }}
  , {{ nullify_empty_strings('mobilephone','mobile_phone') }}
  , {{ nullify_empty_strings('not_p1_reason') }}
  , {{ nullify_empty_strings('hp__addressable_lead','hp_addressable_lead') }}
  , {{ nullify_empty_strings('ev__addressable_lead','ev_addressable_lead') }}
  , {{ nullify_empty_strings('lct__addressable_lead','lct_addressable_lead') }}
  , {{ nullify_empty_strings('hp__fixed_price_quote','hp_fixed_price_quote') }}
  , {{ nullify_empty_strings('recent_building_work_','recent_building_work') }}
  , {{ nullify_empty_strings('roof_direction') }}
  , {{ nullify_empty_strings('roof_features') }}
  , {{ nullify_empty_strings('roof_shape') }}
  , {{ nullify_empty_strings('solar_not_p1_reason') }}
  , {{ nullify_empty_strings('space_for_heat_pump_','space_for_heat_pump') }}
  , {{ nullify_empty_strings('space_for_scaffolding_','space_for_scaffolding') }}
  , {{ nullify_empty_strings('space_for_water_cylinder_','space_for_water_cylinder') }}
  , {{ nullify_empty_strings('state') }}
  , {{ nullify_empty_strings('zip') }}
  , CAST(associate_json AS STRING) AS associate_json
  , CAST(associatedcompanyid AS BIGINT) AS associated_company_id
  , CAST(days_to_close AS BIGINT) AS days_to_close
  , CAST(hubspot_owner_id AS BIGINT) AS hubspot_owner_id
  , CAST(hubspot_team_id AS BIGINT) AS hubspot_team_id
  , CAST(num_associated_deals AS BIGINT) AS num_associated_deals
  , CAST(num_contacted_notes AS BIGINT) AS num_contacted_notes
  , CAST(num_notes AS BIGINT) AS num_notes
  , CAST(number_of_bathrooms AS BIGINT) AS number_of_bathrooms
  , CAST(number_of_bedrooms AS BIGINT) AS number_of_bedrooms
  , CAST(number_of_floors_ AS BIGINT) AS number_of_floors
  , CAST(property_floor_area AS BIGINT) AS property_floor_area
  , CAST(uprn AS BIGINT) AS uprn
  , CAST(price_predictor AS FLOAT) AS price_predictor
  , CAST(recent_deal_amount AS FLOAT) AS recent_deal_amount
  , CAST(total_revenue AS FLOAT) AS total_revenue
  , CAST(archived AS BOOLEAN) AS is_archived
  , CAST(currentlyinworkflow AS BOOLEAN) AS is_currently_in_workflow
  , CAST(epc_cavity_wall_required_ AS BOOLEAN) AS is_epc_cavity_wall_required
  , CAST(epc_loft_insulation_required_ AS BOOLEAN) AS is_epc_loft_insulation_required
  , CAST(f_f_contact AS BOOLEAN) AS is_f_f_contact
  , CAST(heat_pump_p1_ AS BOOLEAN) AS is_heat_pump_p1
  , CAST(loft_extension_ AS BOOLEAN) AS is_loft_extension
  , CAST(in_hp_service_area_ AS BOOLEAN) AS is_in_hp_service_area
  , CAST(planning_required_ AS BOOLEAN) AS is_planning_required
  , CAST(priority_1_ AS BOOLEAN) AS is_priority_1
  , CAST(renovations_scheduled_ AS BOOLEAN) AS is_renovations_scheduled
  , CAST(sent_solar_p1_holding_email AS BOOLEAN) AS is_sent_solar_p1_holding_email
  , TO_UTC_TIMESTAMP(created_at,'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at,'UTC') AS updated_at
  , TO_UTC_TIMESTAMP(closedate,'UTC') AS close_date
  , TO_UTC_TIMESTAMP(createdate,'UTC') AS create_date
  , TO_UTC_TIMESTAMP(engagements_last_meeting_booked,'UTC') AS engagements_last_meeting_booked
  , TO_UTC_TIMESTAMP(first_conversion_date,'UTC') AS first_conversion_date
  , TO_UTC_TIMESTAMP(first_deal_created_date,'UTC') AS first_deal_created_date
  , TO_UTC_TIMESTAMP(hubspot_owner_assigneddate,'UTC') AS hubspot_owner_assigned_date
  , TO_UTC_TIMESTAMP(lastmodifieddate,'UTC') AS last_modified_date
  , TO_UTC_TIMESTAMP(notes_last_contacted,'UTC') AS notes_last_contacted
  , TO_UTC_TIMESTAMP(notes_last_updated,'UTC') AS notes_last_updated
  , TO_UTC_TIMESTAMP(notes_next_activity_date,'UTC') AS notes_next_activity_date
  , TO_UTC_TIMESTAMP(recent_deal_close_date,'UTC') AS recent_deal_close_date
  , DATE(latest_epc_date) AS latest_epc_date
  , DATE(contact_hp_ashp) AS contact_hp_ashp_date
  , DATE(contact_solar_hsd) AS contact_solar_hsd_date
  , DATE(original_enquiry_date) AS original_enquiry_date
  , DATE(property_build_date) AS property_build_date
FROM {{ source('src_hub_spot','hubspot_contacts') }}
