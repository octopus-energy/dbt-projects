SELECT
  id AS history_id
  , _original_id AS user_id
  , {{ nullify_empty_strings('email') }}
  , {{ nullify_empty_strings('given_name') }}
  , {{ nullify_empty_strings('family_name') }}
  , staff_role
  , skill_codes
  , company_id
  , is_staff
  , is_superuser
  , is_active
  , is_verified
  , is_admin
  , history_date
  , history_change_reason
  , history_type
  , history_user_id
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
  , TO_UTC_TIMESTAMP(date_joined, 'UTC') AS joined_at
  , DATE(last_access_date) AS last_access_date
  , TO_UTC_TIMESTAMP(last_login, 'UTC') AS last_login_at
FROM {{ source('src_coconut', 'users_historicaloesuser') }}
