{{ config(
    materialized='view',
    group='hubspot',
    access='private'
) }}

with source_data as (
    -- Replace this with actual source data extraction
    select
        'example_id' as id,
        'example_data' as data_field,
        current_timestamp() as extracted_at,
        current_timestamp() as updated_at
)

select * from source_data

-- This is an example staging model for hubspot_hubspot
-- It should extract and lightly transform data from --   hubspot
-- Following source-aligned data mesh principles
