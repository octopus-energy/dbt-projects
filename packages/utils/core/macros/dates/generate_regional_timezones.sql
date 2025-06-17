{% macro generate_regional_timezones(field_name) %}
  {%- set country_tz = {
    "london": 'Europe/London',
    "paris": 'Europe/Paris',
    "berlin": 'Europe/Berlin',
    "tokyo": 'Asia/Tokyo',
    "wellington": 'Pacific/Auckland',
    "texas": 'America/Chicago',
    "madrid": 'Europe/Madrid'
  } -%}

  {% for country, timezone in country_tz.items() -%}
    {% if not loop.first %},{% endif %} FROM_UTC_TIMESTAMP({{ field_name }}, '{{ timezone }}') AS {{ field_name }}_{{ country }}_tz
  {% endfor %}
{% endmacro %}
