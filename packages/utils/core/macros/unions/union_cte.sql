{% macro union_cte(cte1='', cte2='', union_type='ALL', column_names=[]) %}
    {%- if union_type.upper() not in ['ALL', 'DISTINCT'] -%}
        {{ exceptions.raise_compiler_error("Invalid union_type: must be 'ALL' or 'DISTINCT'.") }}
    {%- endif -%}

    select
    {% for col in column_names %}
        {{ col }}{% if not loop.last %}, {% endif %}
    {% endfor %}
    from {{ cte1 }}

    union {{ union_type }}

    select
    {% for col in column_names %}
        {{ col }}{% if not loop.last %}, {% endif %}
    {% endfor %}
    from {{ cte2 }}
{% endmacro %}
