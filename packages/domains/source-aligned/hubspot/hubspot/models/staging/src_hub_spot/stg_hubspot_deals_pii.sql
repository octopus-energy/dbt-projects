SELECT
  {{ dbt_utils.generate_surrogate_key(
  [
    "deals.id",
    "deals.associate_json",
  ]
  ) }} AS hubspot_deals_contacts_id
  , CAST(deals.id AS BIGINT) AS hubspot_deal_id
  , CAST(CAST(deals.associate_json:id AS STRING) AS BIGINT) AS hubspot_contact_id
  , {{ nullify_empty_strings('bus_application_submitted') }}
  , {{ nullify_empty_strings('closed_lost_reason') }}
  , {{ nullify_empty_strings('lct__closed_lost_reason', 'lct_closed_lost_reason') }}
  , {{ nullify_empty_strings('deal_currency_code') }}
  , {{ nullify_empty_strings('dealname') }}
  , {{ nullify_empty_strings('dealstage') }}
  , {{ nullify_empty_strings('delivery_booking_ref') }}
  , {{ nullify_empty_strings('dno') }}
  , {{ nullify_empty_strings('dno_application') }}
  , {{ nullify_empty_strings('dno_status') }}
  , {{ nullify_empty_strings('dno_status_solar') }}
  , {{ nullify_empty_strings('heat_pump_model') }}
  , {{ nullify_empty_strings('hs_tag_ids') }}
  , {{ nullify_empty_strings('install_crew_assigned') }}
  , {{ nullify_empty_strings('install_status') }}
  , {{ nullify_empty_strings('installation_pack_link') }}
  , {{ nullify_empty_strings('job_outcome_status') }}
  , {{ nullify_empty_strings('kraken_account_id') }}
  , {{ nullify_empty_strings('opensolar_link') }}
  , {{ nullify_empty_strings('lct__pod', 'lct_pod') }}
  , {{ nullify_empty_strings('lct__area', 'lct_area') }}
  , {{ nullify_empty_strings('lct__subregion', 'lct_subregion') }}
  , {{ nullify_empty_strings('lct__region', 'lct_region') }}
  , CASE
    WHEN deals.lct__products__deal_ = 'EV Charger' THEN 'EV'
    WHEN deals.lct__products__deal_ = 'Solar' THEN 'SOLAR'
    WHEN deals.lct__products__deal_ = 'Heat Pump' THEN 'HEATPUMP'
    WHEN deals.lct__products__deal_ = 'GBIS' THEN 'GBIS'
    WHEN deals.lct__products__deal_ = 'no_mcs_export' THEN 'NO_MCS_EXPORT'
    ELSE 'OTHER'
  END AS lct_product
  , {{ nullify_empty_strings('lct__bespoke_items', 'lct_bespoke_items') }}
  , {{ nullify_empty_strings('lct__monetary_incentives', 'lct_monetary_incentives') }}
  , {{ nullify_empty_strings('lct__payment_preference', 'lct_payment_preference') }}
  , {{ nullify_empty_strings('lct__survey_j_ref', 'lct_survey_j_ref') }}
  , {{ nullify_empty_strings('lct__service_incentives', 'lct_service_incentives') }}
  , {{ nullify_empty_strings('ev__preferred_charger__cloned_', 'ev_preferred_charger') }}
  , {{ nullify_empty_strings('ev__vehicle_delivery_date') }}
  , {{ nullify_empty_strings('ev__vehicle_delivered___deal_') }}
  , {{ nullify_empty_strings('ev__oev_pipedrive_id') }}
  , {{ nullify_empty_strings('ev__kraken_purchase_code') }}
  , {{ nullify_empty_strings('ev__partner_referral', 'ev_partner_referral') }}
  , {{ nullify_empty_strings('all__quote__timestamp_') }}
  , {{ nullify_empty_strings('pipeline') }}
  , {{ nullify_empty_strings('postcode') }}
  , {{ nullify_empty_strings('scaffold_dismantled') }}
  , {{ nullify_empty_strings('scaffold_erected_check') }}
  , {{ nullify_empty_strings('scaffolding_sent') }}
  , {{ nullify_empty_strings('scaffolding_type') }}
  , {{ nullify_empty_strings('service_plan') }}
  , {{ nullify_empty_strings('solar_booking_reference') }}
  , {{ nullify_empty_strings('solar_install_crew_assigned') }}
  , {{ nullify_empty_strings('solar_install_status') }}
  , {{ nullify_empty_strings('slr__partnership__deal_') }}
  , {{ nullify_empty_strings('slr_whd__sy') }}
  , {{ nullify_empty_strings('survey_results_link') }}
  , {{ nullify_empty_strings('tech_check_outcome') }}
  , {{ nullify_empty_strings('hp__online_quote_id') }}
  , {{ nullify_empty_strings('trial_type') }}
  , CAST(deals.associate_json AS STRING) AS associate_json
  , CAST(deals.amount AS INT) AS amount
  , CAST(deals.lct__sale_price AS FLOAT) AS lct_sale_price
  , CAST(deals.amount_in_home_currency AS INT) AS amount_in_home_currency
  , CAST(deals.booking_team_owner AS INT) AS booking_team_owner
  , CAST(deals.cval AS INT) AS cval
  , CAST(deals.days_to_close AS INT) AS days_to_close
  , CAST(deals.hp__indicative_price__low_temp_ AS FLOAT) AS hp_indicative_price_low_temp
  , CAST(deals.hp__indicative_price__high_temp_ AS FLOAT) AS hp_indicative_price_high_temp
  , CAST(deals.hp__postsurvey_price__low_temp_ AS FLOAT) AS hp_postsurvey_price_low_temp
  , CAST(deals.hp__postsurvey_price__high_temp_ AS FLOAT) AS hp_postsurvey_price_high_temp
  , CAST(deals.hubspot_owner_id AS INT) AS hubspot_owner_id
  , CAST(deals.hubspot_team_id AS INT) AS hubspot_team_id
  , CAST(deals.lct__incentive_total AS INT) AS lct_incentive_total
  , CAST(deals.num_associated_contacts AS INT) AS num_associated_contacts
  , CAST(deals.num_contacted_notes AS INT) AS num_contacted_notes
  , CAST(deals.num_notes AS INT) AS num_notes
  , CAST(deals.product_specialist AS INT) AS product_specialist
  , CAST(deals.qualifier AS INT) AS qualifier
  , CAST(deals.tech_check_completed_by_ AS INT) AS tech_check_completed_by
  , CAST(deals.tech_validation_team AS INT) AS tech_validation_team
  , CAST(deals.price_predictor AS FLOAT) AS price_predictor
  , CAST(deals.cancelled_post_sale_ AS BOOLEAN) AS is_cancelled_post_sale
  , CAST(deals.dno_sent AS BOOLEAN) AS is_dno_sent
  , CAST(deals.archived AS BOOLEAN) AS is_archived
  , CAST(deals.handover_complete_solar AS BOOLEAN) AS is_handover_complete_solar
  , CAST(deals.install_team_reviewed AS BOOLEAN) AS is_install_team_reviewed
  , CAST(deals.ip_complete AS BOOLEAN) AS is_ip_complete
  , CAST(deals.materials_sent AS BOOLEAN) AS is_materials_sent
  , CAST(deals.mcs_cert_complete AS BOOLEAN) AS is_mcs_cert_complete
  , CAST(deals.payment_received_solar AS BOOLEAN) AS is_payment_received_solar
  , CAST(deals.tech_check_required AS BOOLEAN) AS is_tech_check_required
  , TO_UTC_TIMESTAMP(deals.clawback_date, 'UTC') AS clawback_date
  , TO_UTC_TIMESTAMP(deals.lct__closed_lost__timestamp_, 'UTC') AS close_date
  , TO_UTC_TIMESTAMP(deals.createdate, 'UTC') AS create_date
  , TO_UTC_TIMESTAMP(deals.engagements_last_meeting_booked, 'UTC')
  AS engagements_last_meeting_booked
  , TO_UTC_TIMESTAMP(deals.hubspot_owner_assigneddate, 'UTC') AS hubspot_owner_assigned_date
  , TO_UTC_TIMESTAMP(deals.notes_last_contacted, 'UTC') AS notes_last_contacted
  , TO_UTC_TIMESTAMP(deals.notes_last_updated, 'UTC') AS notes_last_updated
  , TO_UTC_TIMESTAMP(deals.notes_next_activity_date, 'UTC') AS notes_next_activity_date
  , TO_UTC_TIMESTAMP(deals.all__system_live__timestamp_, 'UTC') AS lct_sale_date
  , DATE(deals.commissioning_date) AS commissioning_date
  , DATE(deals.cpaid) AS c_paid
  , DATE(deals.cpaid_install) AS c_paid_install
  , DATE(deals.home_survey_date) AS home_survey_date
  , DATE(deals.install_date) AS install_date
  , DATE(deals.quote_submitted_date) AS quote_submitted_date
  , DATE(deals.scaffold_requested_date) AS scaffold_requested_date
  , DATE(deals.scaffolding_up_date) AS scaffolding_up_date
  , DATE(deals.solar_return_date) AS solar_return_date
  , DATE(deals.tech_check_date) AS tech_check_date
  , DATE(deals.video_consultation_date) AS video_consultation_date
  , TO_UTC_TIMESTAMP(deals.createdat, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(deals.updatedat, 'UTC') AS updated_at
FROM {{ source('src_hub_spot', 'hubspot_deals') }} AS deals
WHERE associate_json LIKE '%deal_to_contact%'
