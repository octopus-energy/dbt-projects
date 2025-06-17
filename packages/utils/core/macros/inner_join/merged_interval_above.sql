{% macro merged_interval_above(above) %}
  LEAST({{ utils_core.concat_affix(above) }})
{% endmacro %}
