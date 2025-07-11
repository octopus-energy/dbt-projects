SELECT
  id AS oes_user_companies_id
  , oesuser_id AS user_id
  , company_id
FROM {{ source('src_coconut', 'users_oesuser_companies') }}
