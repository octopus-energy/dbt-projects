{% macro get_meta_columns(model, package_name, meta_key=none, node_type='model') %}

	{% if execute %}
    
        {% set meta_columns = [] %}
        {% set graph_table_list = graph.nodes.values() | selectattr("resource_type", "equalto", "model") | selectattr("package_name", "equalto", package_name) | selectattr("name", "equalto", model.name) | list %}
        {% if graph_table_list | length < 1 %}
            {% do exceptions.raise_compiler_error("Couldn't find " ~ package_name ~ "." ~ model.name ~ " in graph context variable. Its likely the model exists in a different package to the current project. Make sure the package_name parameter is passed to the macro in the initial invocation. See https://www.notion.so/kraken-tech/Designing-dbt-packages-08c23c0439c84579baaf63a949a2ca2c?pvs=4#8c4b8ff49994416e8068989d236ec842 for more information.") %}
        {% endif %}
        {% set graph_table = graph_table_list | first %}
        {% set columns = graph_table['columns']  %}
        
        {% for column in columns %}
            {% if meta_key is not none %}

                {% if graph_table['columns'][column]['meta'][meta_key] == true %}

                    {# {% do log("Sensitive: " ~ column, info=true) %} #}

                    {% do meta_columns.append(column) %}

                {% endif %}
            {% else %}
                {% do meta_columns.append(column) %}
            {% endif %}
        {% endfor %}
	
        {{ return(meta_columns) }}

	{% endif %}

{% endmacro %}
