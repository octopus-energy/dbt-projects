{% macro intervals_overlap(below, above) %}
  {{ utils_core.merged_interval_below(below) }} < {{ utils_core.merged_interval_above(above) }}
{% endmacro %}
