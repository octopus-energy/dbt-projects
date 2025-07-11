{% snapshot hibob_employee_snapshot %}

{{
    config(
      unique_key='employee_id',
      strategy='check',
      check_cols=[
        'employment_contract'
        ,'employment_status'
        ,'employment_type'
        ,'email_address'
        ,'fte'
        ,'job_title'
        ,'manager_name'
        ,'site'
        ,'department'
        ,'team'
        ,'sub_team'
        ,'team_site'
        ,'team_type'
        ,'secondlevel_manager_name'
        ,'display_name'
      ],
      invalidate_hard_deletes=True
    )
}}

  SELECT *

  FROM {{ source('src_hibob', 'employee_latest') }}

{% endsnapshot %}
