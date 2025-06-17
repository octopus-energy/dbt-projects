{% macro current_timestamp_local(timezone=var('local_timezone')) -%}
  {{ utils_core.localize(utils_core.current_timestamp_utc(), timezone) }}
{%- endmacro %}