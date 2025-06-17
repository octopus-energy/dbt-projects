{% materialization shared_view, adapter='databricks' -%}
  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=schema, database=database,
      type='view') -%}
  {% set grant_config = config.get('grants') %}
  {% set tags = config.get('databricks_tags') %}
  {% set table_name = "%s.%s" | format(this.schema, this.name) %}
  {% do log("Table name: " ~ table_name) %}

  {{ run_hooks(pre_hooks) }}

  {% if target.name == "prod" %}

    {% set share_name_suffix = config.require('share_name_suffix',) %}
    {% set comment = config.get('comment', share_name_suffix.replace("_"," "))%}
    {% set normalised_installation = env_var("FABRIC_INSTALLATION").replace("-", "_") %}
    {% set normalised_internal_group = env_var("FABRIC_INTERNAL_GROUP").replace("-", "_") %}
    {% set normalised_environment = env_var("FABRIC_ENVIRONMENT").replace("-", "_") %}
    {% set share_name_prefix = "%s_%s_%s" | format(normalised_installation, normalised_internal_group, normalised_environment) %}
    {% set share_name = "%s_%s" | format(share_name_prefix, share_name_suffix) %} 
    {% do log("Share name: " ~ share_name) %}
    {% set get_tables_in_share_sql %}
        SHOW ALL IN SHARE `{{ share_name }}`;
    {% endset %}

    {% set tables_in_share = run_query(get_tables_in_share_sql).columns[0].values() %}

    {% if table_name in tables_in_share %}

        {% set remove_view_from_share_sql %}
            ALTER SHARE `{{ share_name }}`
            REMOVE VIEW {{ table_name }}
        {% endset %}

        {% do run_query(remove_view_from_share_sql) %}

        {% do log("Removed '" ~ table_name ~ "'' from '" ~ share_name ~ "'") %}
    {% endif %}
  {% endif %}

  -- If there's a table with the same name and we weren't told to full refresh,
  -- that's an error. If we were told to full refresh, drop it. This behavior differs
  -- for Snowflake and BigQuery, so multiple dispatch is used.
  {%- if old_relation is not none and not old_relation.is_view -%}
    {{ handle_existing_table(should_full_refresh(), old_relation) }}
  {%- endif -%}

  -- build model
  {% call statement('main') -%}
    {{ get_create_view_as_sql(target_relation, sql) }}
  {%- endcall %}

  {% set should_revoke = should_revoke(exists_as_view, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=True) %}

  {%- do apply_tags(target_relation, tags) -%}

  {% if target.name == "prod" %}
      {% set add_view_to_share_sql %}
          ALTER SHARE `{{ share_name }}`
          ADD VIEW {{ table_name }}
          COMMENT "{{ comment }}";
      {% endset %}
      {% do run_query(add_view_to_share_sql) %}
      {% do log("Shared '" ~ table_name ~ "'' with '" ~ share_name ~ "'", info=True) %}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}