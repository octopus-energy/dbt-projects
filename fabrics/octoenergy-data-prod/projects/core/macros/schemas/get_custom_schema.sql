{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %}

{#
    Renders a schema name given a custom schema name. In production, this macro
    will render out the overriden schema name for a model. Otherwise, the default
    schema specified in the active target is used.

    Arguments:
    custom_schema_name: The custom schema name specified for a model, or none
    node: The node the schema is being generated for

#}
{% macro generate_schema_name_for_env(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if node['meta']['sensitive'] -%}

        {%- set sensitive_schema = '_sensitive' -%}

    {%- else -%}

        {%- set sensitive_schema = '' -%}

    {%- endif -%}
    
    {%- if target.name == 'prod' and custom_schema_name is not none -%}

        {{ custom_schema_name | trim }}{{ sensitive_schema }}

    {%- else -%}

        {{ default_schema }}{{ sensitive_schema }}

    {%- endif -%}

{%- endmacro %}