-- Test
WITH emails AS (
  SELECT
    CAST(emails.emailcampaignid AS INT) AS email_campaign_id
    , {{ nullify_empty_strings('recipient') }}
    , TO_UTC_TIMESTAMP(emails.created, 'UTC') AS created_at
  FROM {{ source('src_hub_spot', 'hubspot_emails') }} AS emails
)

SELECT
    {{ dbt_utils.generate_surrogate_key(
    [
      "emails.email_campaign_id",
      "emails.recipient",
      "emails.created_at",
    ]
  ) }} AS hubspot_marketing_email_id
  , emails.email_campaign_id
  , emails.recipient AS email -- PII
  , emails.created_at
FROM emails
