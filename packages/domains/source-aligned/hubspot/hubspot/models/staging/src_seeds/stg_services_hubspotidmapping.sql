SELECT 
  CAST(internal_value AS INT) AS internal_value
  , CAST(display_label AS STRING) AS display_label
  , CAST(pipeline_name AS STRING) AS pipeline_name
  , CAST(display_order AS INT) AS display_order
  , CAST(priority_order AS INT) AS priority_order
FROM {{ ref('services_mapping_hubspotticketdealpipeline') }}
