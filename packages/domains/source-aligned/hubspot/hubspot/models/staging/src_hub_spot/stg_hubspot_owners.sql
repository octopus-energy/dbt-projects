WITH owners AS (
  SELECT
    owners.id AS hubspot_owner_id
    , CAST(owners.user_id AS INT) AS hubspot_user_id
    , CAST(owners.primary_team_id AS INT) AS hubspot_team_id
    , {{ nullify_empty_strings('email') }}
    , {{ nullify_empty_strings('first_name') }}
    , {{ nullify_empty_strings('last_name') }}
    , CAST(owners.primary_team_name AS STRING) AS hubspot_team_name
    , CAST(owners.archived AS BOOLEAN) AS archived
    , CAST(owners.primary_team_primary AS BOOLEAN) AS hubspot_is_team_primary
    , TO_UTC_TIMESTAMP(owners.created_at, 'UTC') AS created_at
    , TO_UTC_TIMESTAMP(owners.updated_at, 'UTC') AS updated_at
  FROM {{ source('src_hub_spot', 'hubspot_owners') }} AS owners
)

SELECT
  owners.hubspot_owner_id
  , owners.hubspot_user_id
  , owners.hubspot_team_id
  , owners.email
  , owners.first_name
  , owners.last_name
  , owners.hubspot_team_name
  , owners.archived
  , owners.hubspot_is_team_primary
  , owners.created_at
  , owners.updated_at
FROM owners
