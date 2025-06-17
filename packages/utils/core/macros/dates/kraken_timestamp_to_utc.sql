{% macro kraken_timestamp_to_utc(timestamp) %}
    TO_UTC_TIMESTAMP({{ timestamp }}, 'UTC')
{% endmacro %}