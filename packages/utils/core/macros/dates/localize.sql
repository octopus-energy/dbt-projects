{% macro localize(timestamp, timezone=var('local_timezone')) %}
    FROM_UTC_TIMESTAMP({{ timestamp }}, '{{ timezone }}')
{% endmacro %}
