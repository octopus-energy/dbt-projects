{{
  config(
    materialized = 'shared_table',
    share_name_suffix="ktl_data_services_share",
  )
}}

  -- Parse the invocation_vars column as JSON
WITH parsed_invocations AS (
    SELECT * EXCEPT(invocation_vars),
           from_json(invocation_vars, 'MAP<STRING, STRING>') AS invocation_vars
    FROM {{ ref('dbt_invocations')}}
)
-- Select from the parsed data
SELECT *, invocation_vars.AIRFLOW__DAG_RUN_ID AS cosmos_invocation_id,
to_timestamp(split(invocation_vars.AIRFLOW__DAG_RUN_ID, '__')[1]) as short_cosmos_invocation_id
FROM parsed_invocations