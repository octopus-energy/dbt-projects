SELECT
  id AS role_event_id
  , role_code
  , assigned_by_id AS assigned_by_user_id
  , revoked_by_id AS revoked_by_user_id
  , user_id
  , TO_UTC_TIMESTAMP(assigned_at, 'UTC') AS assigned_at
  , TO_UTC_TIMESTAMP(revoked_at, 'UTC') AS revoked_at
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
FROM {{ source('src_coconut', 'users_oesuserroleassignment') }}
