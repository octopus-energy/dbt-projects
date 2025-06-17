{% macro concat_affix(list, delim=', ', prefix='', suffix='') %}
    {%- if list -%}
        {# Avoid printing a lone prefix if there are no elements in the list #}
        {{- prefix -}}{{- list | join(suffix + delim + prefix) -}}{{- suffix -}}
    {%- endif-%}
{% endmacro %}



