{% macro date_between(date, below, above) %}
COALESCE({{below}}, {{ var('reporting_start_timestamp') }}) <= {{ date }}
AND {{ date }} < COALESCE({{above}}, {{ var('distant_future_timestamp') }})
{% endmacro %}
