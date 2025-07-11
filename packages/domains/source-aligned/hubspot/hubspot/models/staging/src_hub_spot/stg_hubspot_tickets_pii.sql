WITH tickets AS (
  SELECT
    CAST(ticks.id AS BIGINT) AS hubspot_ticket_id
    , {{ nullify_empty_strings('bus_application_status') }}
    , {{ nullify_empty_strings('bus_gid') }}
    , {{ nullify_empty_strings('content') }}
    , {{ nullify_empty_strings('customer_pack_link') }}
    , {{ nullify_empty_strings('delivery_booking_ref') }}
    , {{ nullify_empty_strings('dno') }}
    , {{ nullify_empty_strings('ev__charger_to_install') }}
    , {{ nullify_empty_strings('ev__assigned_engineer__holding_') }}
    , {{ nullify_empty_strings('ev__booked_engineer') }}
    , {{ nullify_empty_strings('ev__engineer__dropdown_') }}
    , {{ nullify_empty_strings('existing_fuel_type') }}
    , {{ nullify_empty_strings('first_name') }}
    , {{ nullify_empty_strings('fuse_rating') }}
    , {{ nullify_empty_strings('heat_pump_dno_status') }}
    , {{ nullify_empty_strings('heat_pump_install_crew_assigned') }}
    , {{ nullify_empty_strings('heat_pump_scaffolding_type') }}
    , {{ nullify_empty_strings('heat_pump_service_plan') }}
    , {{ nullify_empty_strings('installation_docs_received_') }}
    , {{ nullify_empty_strings('job_outcome_status') }}
    , {{ nullify_empty_strings('kraken_account_id') }}
    , {{ nullify_empty_strings('last_name') }}
    , {{ nullify_empty_strings('lead_engineer') }}
    , {{ nullify_empty_strings('opensolar_link') }}
    , {{ nullify_empty_strings('payment_status') }}
    , {{ nullify_empty_strings('postcode') }}
    , {{ nullify_empty_strings('return_visit_required_') }}
    , {{ nullify_empty_strings('scaffold_dismantled') }}
    , {{ nullify_empty_strings('scaffold_erected_check') }}
    , {{ nullify_empty_strings('post_sale_pre_install_cancellation_reason') }}
    , {{ nullify_empty_strings('scaffolding_type') }}
    , {{ nullify_empty_strings('solar_booking_reference') }}
    , {{ nullify_empty_strings('solar_dno_status') }}
    , {{ nullify_empty_strings('solar_install_crew_assigned') }}
    , {{ nullify_empty_strings('subject') }}
    , {{ nullify_empty_strings('survey_results_link') }}
    , {{ nullify_empty_strings('type_of_solar_dno_required') }}
    , {{ nullify_empty_strings('lct__install_readiness_activity') }}
    , {{ nullify_empty_strings('dno_work_required') }}
    , {{ nullify_empty_strings('ev__post_payment_form_submitted_') }}
    , {{ nullify_empty_strings('lct__install_readiness_underway__timestamp_') }}
    , {{ nullify_empty_strings('lct__scheduling_install') }}
    , {{ nullify_empty_strings('lct__install_booked') }}
    , {{ nullify_empty_strings('lct__days_to_install__ticket_') }}
    , {{ nullify_empty_strings('lct__install_ready_to_schedule_') }}
    , {{ nullify_empty_strings('lct__products__ticket_') }}
    , {{ nullify_empty_strings('lct__install_readiness_checklist') }}
    , {{ nullify_empty_strings('lct__in_day_installation_status') }}
    , {{ nullify_empty_strings('hp__install_readiness_complexity_score') }}
    , {{ nullify_empty_strings('ev__scheduling_status') }}
    , {{ nullify_empty_strings('metering_work_required_') }}
    , {{ nullify_empty_strings('aftercare__product') }}
    , {{ nullify_empty_strings('ac__ticket_type') }}
    , {{ nullify_empty_strings('ac__ev_fault_categories') }}
    , {{ nullify_empty_strings('ac__hp_fault_categories') }}
    , {{ nullify_empty_strings('ac__slr_fault_categories') }}
    , {{ nullify_empty_strings('ac__reason_raised') }}
    , {{ nullify_empty_strings('hp__engineer') }}
    , {{ nullify_empty_strings('hp__electrician') }}
    , {{ nullify_empty_strings('hp__plumber') }}
    , {{ nullify_empty_strings('hp__missing_parts') }}
    , {{ nullify_empty_strings('hp__incorrect_parts') }}
    , {{ nullify_empty_strings('hp__excess_parts') }}
    , {{ nullify_empty_strings('hp__heat_pump_model') }}
    , {{ nullify_empty_strings('hp__actual_install_length') }}
    , {{ nullify_empty_strings('slr__scaff_request_form_complete_') }}
    , {{ nullify_empty_strings('slr__scaffolding_company') }}
    , CAST(ticks.hs_pipeline AS STRING) AS hs_pipeline
    , CAST(ticks.hs_pipeline_stage AS STRING) AS hs_pipeline_stage
    , CAST(ticks.associate_json:id AS STRING) AS hubspot_deal_id
    , CAST(ticks.associate_json AS STRING) AS associate_json
    , CAST(ticks.booking_team_owner AS BIGINT) AS booking_team_owner
    , CAST(ticks.hubspot_owner_id AS STRING) AS hubspot_owner_id
    , CAST(ticks.hubspot_team_id AS STRING) AS hubspot_team_id
    , CAST(ticks.num_contacted_notes AS INT) AS num_contacted_notes
    , CAST(ticks.hp__actual_install_length AS INT) AS hp_actual_install_length
    , CAST(ticks.num_notes AS INT) AS num_notes
    , CAST(ticks.tech_validation_team AS BIGINT) AS tech_validation_team
    , CAST(ticks.time_to_close AS BIGINT) AS time_to_close_seconds
    , CAST(ticks.time_to_first_agent_reply AS BIGINT) AS time_to_first_agent_reply_seconds
    , CAST(ticks.archived AS BOOLEAN) AS is_archived
    , CAST(ticks.design_to_lead_installer AS BOOLEAN) AS is_design_to_lead_installer
    , CAST(ticks.eic_sent AS BOOLEAN) AS is_eic_sent
    , CAST(ticks.handover_pack_sent_ AS BOOLEAN) AS is_handover_pack_sent_
    , CAST(ticks.install_team_reviewed AS BOOLEAN) AS is_install_team_reviewed
    , CAST(ticks.ip_complete AS BOOLEAN) AS is_ip_complete
    , CAST(ticks.materials_sent AS BOOLEAN) AS is_materials_sent
    , CAST(ticks.mcs_cert_complete AS BOOLEAN) AS is_mcs_cert_complete
    , CAST(ticks.scaffolding_design_accepted AS BOOLEAN) AS is_scaffolding_design_accepted
    , CAST(ticks.service_plan_in_kraken_ AS BOOLEAN) AS is_service_plan_in_kraken_
    , CAST(ticks.solar_dno_sent AS BOOLEAN) AS is_solar_dno_sent
    , CAST(ticks.solar_handover_complete AS BOOLEAN) AS is_solar_handover_complete
    , CAST(ticks.solar_payment_received AS BOOLEAN) AS is_solar_payment_received
    , CAST(ticks.warranty_registered AS BOOLEAN) AS is_warranty_registered
    , CAST(ticks.ev_extended_warranty AS STRING) AS ev_extended_warranty
    , TO_UTC_TIMESTAMP(ticks.createdat, 'UTC') AS create_at
    , TO_UTC_TIMESTAMP(ticks.updatedat, 'UTC') AS update_at
    , TO_UTC_TIMESTAMP(ticks.closed_date, 'UTC') AS closed_date
    , TO_UTC_TIMESTAMP(ticks.commissioning_date, 'UTC') AS commissioning_date
    , TO_UTC_TIMESTAMP(ticks.createdate, 'UTC') AS create_date
    , TO_UTC_TIMESTAMP(ticks.first_agent_reply_date, 'UTC') AS first_agent_reply_date
    , TO_UTC_TIMESTAMP(ticks.hubspot_owner_assigneddate, 'UTC') AS hubspot_owner_assigned_date
    , TO_UTC_TIMESTAMP(ticks.last_engagement_date, 'UTC') AS last_engagement_date
    , TO_UTC_TIMESTAMP(ticks.last_reply_date, 'UTC') AS last_reply_date
    , TO_UTC_TIMESTAMP(ticks.notes_last_contacted, 'UTC') AS notes_last_contacted
    , TO_UTC_TIMESTAMP(ticks.notes_last_updated, 'UTC') AS notes_last_updated
    , TO_UTC_TIMESTAMP(ticks.notes_next_activity_date, 'UTC') AS notes_next_activity_date
    , TO_UTC_TIMESTAMP(
      ticks.slr__scaffolding_erection_date, 'UTC'
    ) AS slr_scaffolding_erection_date
    , DATE(ticks.install_date) AS install_date
    , DATE(ticks.post_sale_pre_install_cancellation_date)
    AS post_sale_pre_install_cancellation_date
    , DATE(ticks.post_sale_resurvey_date) AS post_sale_resurvey_date
    , DATE(ticks.scaffold_requested_date) AS scaffold_requested_date
    , DATE(ticks.scaffolding_up_date) AS scaffolding_up_date
    , DATE(ticks.solar_return_date) AS solar_return_date
  FROM {{ source('src_hub_spot', 'hubspot_tickets') }} AS ticks
)

SELECT
  {{ dbt_utils.generate_surrogate_key(
    [
      "tickets.hubspot_ticket_id",
      "tickets.associate_json",
    ]
  ) }} AS hubspot_ticket_deals_id
  , tickets.hubspot_ticket_id
  , CAST(tickets.hubspot_deal_id AS BIGINT) AS hubspot_deal_id
  , tickets.bus_application_status
  , tickets.bus_gid
  , tickets.content
  , tickets.customer_pack_link
  , tickets.delivery_booking_ref
  , tickets.dno
  , tickets.ev__charger_to_install AS ev_charger_to_install
  , COALESCE(
    tickets.ev__assigned_engineer__holding_
    , tickets.ev__booked_engineer
    , tickets.ev__engineer__dropdown_
  ) AS ev_engineer
  , tickets.first_name  -- PII
  , tickets.fuse_rating
  , tickets.heat_pump_dno_status
  , tickets.heat_pump_install_crew_assigned
  , tickets.heat_pump_scaffolding_type
  , tickets.heat_pump_service_plan
  , tickets.hs_pipeline_stage AS pipeline_stage
  , tickets.hs_pipeline AS pipeline
  , tickets.installation_docs_received_ AS installation_docs_received
  , tickets.job_outcome_status
  , tickets.kraken_account_id AS kraken_account_number
  , tickets.last_name  -- PII
  , tickets.lead_engineer
  , tickets.hp__engineer AS hp_engineer
  , tickets.hp__electrician AS hp_electrician
  , tickets.hp__plumber AS hp_plumber
  , tickets.hp__missing_parts AS hp_missing_parts
  , tickets.hp__incorrect_parts AS hp_incorrect_parts
  , tickets.hp__excess_parts AS hp_excess_parts
  , tickets.hp__heat_pump_model AS hp_heat_pump_model
  , tickets.hp__actual_install_length AS hp_actual_install_length
  , tickets.opensolar_link
  , REGEXP_EXTRACT(tickets.opensolar_link, '(\\d+)') AS opensolar_project_id
  , tickets.payment_status
  , tickets.postcode  -- PII
  , tickets.return_visit_required_ AS return_visit_required
  , tickets.scaffold_dismantled
  , tickets.scaffold_erected_check
  , tickets.post_sale_pre_install_cancellation_reason
  , tickets.scaffolding_type
  , tickets.solar_booking_reference
  , tickets.solar_dno_status
  , tickets.solar_install_crew_assigned
  , tickets.subject
  , tickets.survey_results_link
  , tickets.type_of_solar_dno_required
  , tickets.associate_json
  , tickets.booking_team_owner
  , tickets.hubspot_owner_id
  , tickets.hubspot_team_id
  , tickets.num_contacted_notes
  , tickets.num_notes
  , tickets.tech_validation_team
  , tickets.time_to_close_seconds
  , tickets.time_to_first_agent_reply_seconds
  , tickets.is_archived
  , tickets.is_design_to_lead_installer
  , tickets.is_eic_sent
  , tickets.is_handover_pack_sent_
  , tickets.is_install_team_reviewed
  , tickets.is_ip_complete
  , tickets.is_materials_sent
  , tickets.is_mcs_cert_complete
  , tickets.is_scaffolding_design_accepted
  , tickets.is_service_plan_in_kraken_
  , tickets.is_solar_dno_sent
  , tickets.is_solar_handover_complete
  , tickets.is_solar_payment_received
  , tickets.is_warranty_registered
  , tickets.create_at
  , tickets.update_at
  , tickets.closed_date
  , tickets.commissioning_date
  , tickets.create_date
  , tickets.first_agent_reply_date
  , tickets.hubspot_owner_assigned_date
  , tickets.last_engagement_date
  , tickets.last_reply_date
  , tickets.notes_last_contacted
  , tickets.notes_last_updated
  , tickets.notes_next_activity_date
  , tickets.install_date
  , tickets.post_sale_pre_install_cancellation_date
  , tickets.post_sale_resurvey_date
  , tickets.scaffold_requested_date
  , tickets.scaffolding_up_date
  , tickets.solar_return_date
  , tickets.slr__scaff_request_form_complete_ AS slr_scaff_request_form_complete
  , tickets.slr__scaffolding_company AS slr_scaffolding_company
  , tickets.slr_scaffolding_erection_date
  , tickets.lct__install_readiness_activity
  , tickets.lct__install_readiness_checklist AS lct_install_readiness_checklist
  , tickets.dno_work_required
  , tickets.ev__post_payment_form_submitted_
  , tickets.lct__install_readiness_underway__timestamp_
  , tickets.lct__scheduling_install
  , tickets.lct__install_booked
  , tickets.lct__days_to_install__ticket_ AS lct_days_to_install_ticket
  , tickets.lct__install_ready_to_schedule_ AS lct_install_ready_to_schedule
  , tickets.lct__products__ticket_ AS lct_product
  , tickets.ev__scheduling_status
  , tickets.ev_extended_warranty
  , tickets.metering_work_required_ AS metering_work_required
  , tickets.lct__in_day_installation_status AS lct_in_day_installation_status
  , tickets.aftercare__product AS aftercare_product
  , SPLIT(tickets.ac__hp_fault_categories, ';') AS ac_hp_fault_categories
  , SPLIT(tickets.ac__ev_fault_categories, ';') AS ac_ev_fault_categories
  , SPLIT(tickets.ac__slr_fault_categories, ';') AS ac_slr_fault_categories
  , tickets.ac__reason_raised AS ac_reason_raised
  , tickets.ac__ticket_type AS ac_ticket_type
  , tickets.existing_fuel_type
FROM tickets
