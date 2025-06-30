{{
  config(
    materialized = 'shared_table',
    share_name_suffix="ktl_data_services_share",
  )
}}

SELECT
  * FROM {{ ref('alerts_dbt_source_freshness')}}