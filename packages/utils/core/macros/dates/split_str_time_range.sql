{% macro split_str_time_range(time_range_str_col, start_period=True) %}
  IF({{ start_period }},
  SPLIT(SPLIT({{ time_range_str_col }}, ',')[0], '\"')[1],
  COALESCE(SPLIT(SPLIT({{  time_range_str_col }}, ',')[1], '\"')[1], {{ var('distant_future_timestamp') }})
  )
{% endmacro %}