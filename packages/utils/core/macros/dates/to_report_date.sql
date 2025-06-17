{% macro to_report_date(date) %}
	IF( {{ date }} > LAST_DAY( ADD_MONTHS(
		  {{ utils_core.current_timestamp_local() }}, -1)
		), 
		{{ utils_core.current_date() }}, 
		LAST_DAY( {{ date }} )
	)
{% endmacro %}