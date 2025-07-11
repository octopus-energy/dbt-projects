SELECT
  dbt_scd_id AS hibob_snapshot_id
  , employee_id
  , work_id
  , manager_work_id
  , {{ nullify_empty_strings('full_name') }}
  , {{ nullify_empty_strings('display_name') }}
  -- 2023-06-14 - We have found that some users employee_id have
  -- retrospectively been added to a user with the same email address
  -- this CASE will fix that for these 2 users and shouldn't need to be updated further. 
  , CASE employee_id
    WHEN 3024361998075823095
      THEN 'james.hardy1@octoenergy.com'
    WHEN 3024361950545969940
      THEN 'leanne.martin1@octoenergy.com'
    ELSE NULLIF(email_address, '')
  END AS email_address
  , {{ nullify_empty_strings('job_title') }}
  , {{ nullify_empty_strings('department') }}
  , {{ nullify_empty_strings('team') }}
  , {{ nullify_empty_strings('sub_team') }}
  , {{ nullify_empty_strings('team_type') }}
  , {{ nullify_empty_strings('team_site') }}
  , {{ nullify_empty_strings('site') }}
  , {{ nullify_empty_strings('country') }}
  , {{ nullify_empty_strings('manager_name', 'firstlevel_manager_name') }}
  , {{ nullify_empty_strings('manager_work_id', 'firstlevel_manager_work_id') }}
  , {{ nullify_empty_strings('manager_employee_id', 'firstlevel_manager_employee_id') }}
  , {{ nullify_empty_strings('secondlevel_manager_name') }}
  , {{ nullify_empty_strings('secondlevel_manager_employee_id') }}
  , {{ nullify_empty_strings('is_manager') }}
  , NULLIF(fte, 0) AS contractual_fte
  , {{ nullify_empty_strings('business_unit') }}
  , {{ nullify_empty_strings('cost_centre') }}
  , {{ nullify_empty_strings('employment_status') }}
  , {{ nullify_empty_strings('employment_type') }}
  , {{ nullify_empty_strings('employment_contract') }}
  , TO_DATE(start_date) AS start_date
  , TO_DATE(role_start_date) AS role_start_date
  , TO_DATE(probation_end_date) AS probation_end_date
  , TO_DATE(report_date) AS report_date
  ,{{ first_snapshot_from_created_at(snapshot_unique_key="employee_id", created_at='''to_timestamp(start_date)''') }} AS dbt_valid_from -- noqa: L016
  , dbt_valid_to
  ,{{ is_currently_active_record('dbt_valid_from', 'dbt_valid_to') }} AS is_currently_active_record
FROM {{ ref('hibob_employee_snapshot') }}
