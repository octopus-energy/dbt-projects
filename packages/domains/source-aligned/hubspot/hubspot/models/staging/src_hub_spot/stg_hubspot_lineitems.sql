SELECT 
  line_items.id::BIGINT AS hubspot_line_item_id
  , {{ extract_associated_ids(
        column_name='line_items.associate_json',
        entities=['deals'],
        is_string_col=false
    ) }}
  , line_items.name::STRING AS hubspot_line_item 
  , line_items.price::INT AS hubspot_line_item_price
  , line_items.quantity::INT AS hubspot_line_item_quantity
  , TO_UTC_TIMESTAMP(line_items.created_at, 'UTC') AS hubspot_line_item_created_at
  , TO_UTC_TIMESTAMP(line_items.updated_at, 'UTC') AS hubspot_line_item_updated_at

FROM {{ source('src_hub_spot', 'hubspot_line_items') }} AS line_items
