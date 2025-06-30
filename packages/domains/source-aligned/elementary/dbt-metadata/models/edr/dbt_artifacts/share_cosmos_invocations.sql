{{
  config(
    materialized = 'shared_table',
    share_name_suffix="ktl_data_services_share",
  )
}}

with cosmos_invocations as (
    SELECT short_cosmos_invocation_id,
           MIN(to_timestamp(run_started_at)) AS run_started_at,
           MAX(to_timestamp(run_completed_at)) AS run_completed_at
    FROM {{ ref('share_dbt_invocations')}}
    GROUP BY 1
)
SELECT 
    short_cosmos_invocation_id,
    run_started_at,
    run_completed_at,
    unix_timestamp(run_completed_at) - unix_timestamp(run_started_at) AS invocation_duration_seconds,
    (unix_timestamp(run_completed_at) - unix_timestamp(run_started_at)) / 60 AS invocation_duration_minutes
FROM cosmos_invocations