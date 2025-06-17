{% macro nullify_empty_strings(value, new_col_name=value) %}
    {% if not new_col_name %}
        NULLIF({{value}}, '') as {{value}}
    {% else %}
        NULLIF({{value}}, '') as {{new_col_name}}
    {% endif %}

{% endmacro %}