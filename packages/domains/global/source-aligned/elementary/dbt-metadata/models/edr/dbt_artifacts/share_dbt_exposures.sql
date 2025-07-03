{{
  config(
    materialized = 'shared_table',
    share_name_suffix="ktl_data_services_share",
  )
}}

WITH ranked_models AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY generated_at DESC) AS row_num
    FROM {{ ref('dbt_exposures')}}
)
SELECT * EXCEPT (row_num)
FROM ranked_models
WHERE row_num = 1;