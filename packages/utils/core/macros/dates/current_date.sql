{% macro current_date(timezone=var('local_timezone')) -%}
  TO_DATE({{ utils_core.current_timestamp_local(timezone) }})
{%- endmacro %}
