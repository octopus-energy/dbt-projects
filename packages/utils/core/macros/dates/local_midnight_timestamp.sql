{% macro local_midnight_timestamp(timestamp, timezone=var('local_timezone')) %}
    to_utc_timestamp(to_date(from_utc_timestamp({{timestamp}}, '{{timezone}}')), '{{timezone}}')
{% endmacro %}