{% macro get_salt(column_name) %}
    {{ return( env_var("UTILS_PII_SALT") ) }}
{% endmacro %}
