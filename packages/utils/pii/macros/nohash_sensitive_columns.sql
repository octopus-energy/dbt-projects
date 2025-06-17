{% macro nohash_sensitive_columns(source_table, join_key=none, package_name=context['project_name']) %}

    {% set meta_columns = utils_pii.get_meta_columns(source_table, package_name, meta_key="sensitive") %}

    {% if join_key is not none -%}
        {{ utils_pii.hash_of_column(join_key) }}
    {%- endif  %}

    {%- for column in meta_columns %}
        {{ column }}  {% if not loop.last %} , {% endif %}
    {%- endfor %}

{% endmacro %}