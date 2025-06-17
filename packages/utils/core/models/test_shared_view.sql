{{
    config(
        materialized='shared_view',
        share_name_suffix = 'ktl_data_services_share',
        enabled=False
    )
}}

SELECT 1