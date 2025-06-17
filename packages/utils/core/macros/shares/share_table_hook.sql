{%- macro share_table_hook(share_name_suffix) -%}

{% if target.name == "prod" %}
  {% set table_name = "%s.%s" | format(this.schema, this.name) %}
  {% do log("Table name: " ~ table_name) %}
  {% set comment =  share_name_suffix.replace("_"," ") %}
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
    {{ tables_in_share }}
  {% if table_name not in tables_in_share %}
          ALTER SHARE `{{ share_name }}`
          ADD TABLE {{ table_name }}
          COMMENT "{{ comment }}"
          WITH HISTORY;
      {% do log("Shared '" ~ table_name ~ "'' with '" ~ share_name ~ "'", info=True) %}
  {% else %}
      {% do log("Table '" ~ table_name ~ "'' already shared with '" ~ share_name ~ "'") %}
  {% endif %}
{% endif %}

{%- endmacro -%}
