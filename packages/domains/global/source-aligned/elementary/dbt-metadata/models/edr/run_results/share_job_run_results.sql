{{
  config(
    materialized = 'table',
    share_name_suffix="ktl_data_services_share",
  )
}}

SELECT
  * FROM {{ ref('job_run_results')}}