{% materialization shared_table, adapter = 'databricks', supported_languages=['sql', 'python'] %}
  {{ log("MATERIALIZING TABLE") }}
  {%- set language = model['language'] -%}
  {%- set identifier = model['alias'] -%}
  {%- set grant_config = config.get('grants') -%}
  {%- set tblproperties = config.get('tblproperties') -%}
  {%- set tags = config.get('databricks_tags') -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier, needs_information=True) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  -- setup: if the target relation already exists, drop it
  -- in case if the existing and future table is delta, we want to do a
  -- create or replace table instead of dropping, so we don't have the table unavailable
  {% if old_relation and (not (old_relation.is_delta and config.get('file_format', default='delta') == 'delta')) or (old_relation.is_materialized_view or old_relation.is_streaming_table) -%}
    {{ adapter.drop_relation(old_relation) }}
  {%- endif %}

  -- build model

  {%- call statement('main', language=language) -%}
    {{ create_table_as(False, target_relation, compiled_code, language) }}
  {%- endcall -%}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}
  {% if language=="python" %}
    {% do apply_tblproperties(target_relation, tblproperties) %}
  {% endif %}

  {%- do apply_tags(target_relation, tags) -%}
  
  {% do persist_docs(target_relation, model, for_relation=language=='python') %}

  {% do persist_constraints(target_relation, model) %}

  {% do optimize(target_relation) %}

  {% if target.name == "prod" %}

      {% set table_name = "%s.%s" | format(this.schema, this.name) %}
      {% do log("Table name: " ~ table_name) %}
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
      {% if table_name not in tables_in_share %}
          {% set add_table_to_share_sql %}
              ALTER SHARE `{{ share_name }}`
              ADD TABLE {{ table_name }}
              COMMENT "{{ comment }}"
              WITH HISTORY;
          {% endset %}
          {% do run_query(add_table_to_share_sql) %}
          {% do log("Shared '" ~ table_name ~ "'' with '" ~ share_name ~ "'", info=True) %}
      {% else %}
          {% do log("Table '" ~ table_name ~ "'' already shared with '" ~ share_name ~ "'") %}
      {% endif %}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{% endmaterialization %}