SELECT
  id AS project_crm_link_id
  , project_id
  , crmlink_id
FROM {{ source('src_coconut', 'projects_project_crm_links') }}
