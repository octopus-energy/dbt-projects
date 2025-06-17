{% macro distinct_on(table, grouping_columns, validation_columns, value_columns, method='MAX', to_filter=False) %}
{% if method not in ['MAX','MIN'] %}
    {{ exceptions.raise_compiler_error("`method` argument for distinct_on macro must be one of ['MAX', 'MIN'] Got: '" ~ method ~"'.'") }}
{% else %}
    SELECT {{ concat_affix(grouping_columns,',') }},
    {% for value_col in value_columns %}
        {%- if to_filter -%}
           {{method}}_BY({{ value_col }},({{ concat_affix(validation_columns,delim = ',')}}))
           FILTER ( WHERE {{ value_col }} is NOT NULL ) AS {{ value_col }}
        {%- else -%}
           {{method}}_BY({{ value_col }},({{ concat_affix(validation_columns,delim = ',')}})) AS {{ value_col }}
        {%- endif -%}
        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}
    ,
    {% for validation_col in validation_columns %}
        {{method}}_BY({{ validation_col }},({{ concat_affix(validation_columns,delim = ',')}})) AS {{ validation_col }}
        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}
    FROM {{table}}
    GROUP BY {{ concat_affix(grouping_columns,',') }}

{% endif %}
{% endmacro %}