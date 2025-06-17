{% macro current_timestamp_utc() -%}
  CURRENT_TIMESTAMP()
{%- endmacro %}