SELECT
  id AS user_id
  , {{ nullify_empty_strings('password') }} -- PII
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
  , TO_UTC_TIMESTAMP(created_at, 'UTC') AS created_at
  , TO_UTC_TIMESTAMP(updated_at, 'UTC') AS updated_at
  , TO_UTC_TIMESTAMP(date_joined, 'UTC') AS joined_at
  , TO_UTC_TIMESTAMP(last_access_date, 'UTC') AS last_access_date
  , TO_UTC_TIMESTAMP(last_login, 'UTC') AS last_login_at
FROM {{ source('src_coconut', 'users_oesuser') }}
