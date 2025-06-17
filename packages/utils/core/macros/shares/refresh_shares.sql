{% macro refresh_shares() %}

  {{ log("Refreshing shares...", info=True) }}

  {% set catalogs_query %}
    SHOW CATALOGS
  {% endset %}

  {% set catalogs_list = run_query(catalogs_query) %}

  {% for catalog_entry in catalogs_list %}

    {% if '_services_share' in catalog_entry.catalog %}
      {{ log("Refreshing shares for catalog: " ~ catalog_entry.catalog ~ "...") }}

      {% set schemas_query %}
        SHOW SCHEMAS IN {{catalog_entry.catalog}}
      {% endset %}

      {% for schema_entry in run_query(schemas_query) %}

        {{log("Refreshing shares for schema: " ~ schema_entry.databaseName)}}

        {% set tables_query %}
          SHOW TABLES IN {{catalog_entry.catalog}}.{{schema_entry.databaseName}}
        {% endset %}

        {% set tables_list = run_query(tables_query) %}

      {% endfor %}

      {{ log("Shares refreshed for catalog: " ~ catalog_entry.catalog ~ ".") }}

    {% endif %}

  {% endfor %}

  {{ log("Shares refreshed.", info=True) }}

{%- endmacro -%}