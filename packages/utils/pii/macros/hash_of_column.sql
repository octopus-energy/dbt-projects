{% macro hash_of_column(column, rename_col=true) %}

    SHA2(
        TRIM(
            LOWER(
                CAST({{ column|lower }} AS {{ type_string() }})
                || '{{ utils_pii.get_salt(column|lower) }}'
            )
        ),
        256
    ) {% if rename_col %} AS {{column|lower}}_hash, {% endif %}

{% endmacro %}
