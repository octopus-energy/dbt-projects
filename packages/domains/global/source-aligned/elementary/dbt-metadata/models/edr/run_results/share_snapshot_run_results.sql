{{
  config(
    materialized = 'shared_table',
    share_name_suffix="ktl_data_services_share",
  )
}}

SELECT
  * FROM {{ ref('snapshot_run_results')}}