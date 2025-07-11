SELECT -- noqa: L034
  hibob_snapshot_id
  , employee_id
  , work_id
  , manager_work_id
  , full_name
  , display_name
  , email_address
  , job_title
  , department
  , team
  , sub_team
  , CASE
    WHEN UPPER(SUBSTRING_INDEX(sub_team, ' ', 1)) IN ('DIGIOPS', 'TEAM')
      THEN UPPER(SUBSTRING_INDEX(sub_team, ' ', -1))
    ELSE sub_team
  END
  AS refactored_sub_team
  , team_type
  , team_site
  , site
  , country
  , firstlevel_manager_name
  , firstlevel_manager_work_id
  , firstlevel_manager_employee_id
  , secondlevel_manager_name
  , secondlevel_manager_employee_id
  , is_manager
  , contractual_fte
  , employment_status
  , employment_type
  , employment_contract
  , start_date
  , role_start_date
  , probation_end_date
  , report_date
  , business_unit
  , cost_centre
  , dbt_valid_from
  , dbt_valid_to
  , is_currently_active_record
FROM {{ ref('stg_hibob_employee_snapshot') }}
