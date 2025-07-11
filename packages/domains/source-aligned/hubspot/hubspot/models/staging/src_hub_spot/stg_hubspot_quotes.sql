WITH quotes AS (
  SELECT
    quotes.id AS hubspot_quote_id
    , {{ nullify_empty_strings('hs_payment_status') }}
    , {{ nullify_empty_strings('hs_title') }}
    , CAST(quotes.associate_json:id AS STRING) AS hubspot_deal_id
    , quotes.hs_quote_amount AS quote_amount
    -- Note 'dates' below are actually timestamps in source data
    , TO_UTC_TIMESTAMP(quotes.hs_esign_date, 'UTC') AS quote_signed_at
    , TO_UTC_TIMESTAMP(quotes.hs_payment_date, 'UTC') AS quote_paid_at
    , TO_UTC_TIMESTAMP(quotes.hs_expiration_date, 'UTC') AS quote_expires_at
    , TO_UTC_TIMESTAMP(quotes.hs_lastmodifieddate, 'UTC') AS quote_last_modified_at
    , TO_UTC_TIMESTAMP(quotes.createdat, 'UTC') AS created_at
    , TO_UTC_TIMESTAMP(quotes.updatedat, 'UTC') AS updated_at
  FROM {{ source('src_hub_spot', 'hubspot_quotes') }} AS quotes
)

SELECT
  quotes.hubspot_quote_id
  , CAST(quotes.hubspot_deal_id AS BIGINT) AS hubspot_deal_id
  , quotes.hs_payment_status AS quote_payment_status
  , quotes.hs_title AS quote_title
  , quotes.quote_amount
  , quotes.quote_signed_at
  , quotes.quote_paid_at
  , quotes.quote_expires_at
  , quotes.quote_last_modified_at
  , quotes.created_at
  , quotes.updated_at
FROM quotes
