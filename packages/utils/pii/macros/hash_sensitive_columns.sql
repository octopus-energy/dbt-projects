{% macro hash_sensitive_columns(source_table, package_name=context['project_name'], meta_key="sensitive", node_type="model") %}

    {% set pii_columns = utils_pii.get_meta_columns(source_table, package_name, meta_key, node_type) %}

    {%- for column in pii_columns %}
        {{ utils_pii.hash_of_column(column) }}
    {% endfor %}

    {{ dbt_utils.star(from=source_table, except=pii_columns) }}

{% endmacro %}
