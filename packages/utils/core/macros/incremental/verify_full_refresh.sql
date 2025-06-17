{%- macro verify_full_refresh() %}
    {% if execute %}
        {% if target.name not in ('dev','testing') %}
            {% if flags.FULL_REFRESH and not var('allow_full_refresh', False) %}
                {{ exceptions.raise_compiler_error("Full refresh in production is not allowed for this model unless the argument \"--vars 'allow_full_refresh: True'\" is included in the dbt run command.") }}
            {% endif %}
        {% endif %}
    {% endif %}
{%- endmacro %}
