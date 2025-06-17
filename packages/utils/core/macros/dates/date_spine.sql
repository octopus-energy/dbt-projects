{% macro date_spine(datepart, start_date, end_date, granularity=1) %}

SELECT explode(sequence(to_timestamp('{{start_date}}'), to_timestamp('{{end_date}}'), interval {{granularity}} {{datepart}})) as date_{{datepart}}

{% endmacro %}
