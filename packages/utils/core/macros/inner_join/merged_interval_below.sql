{% macro merged_interval_below(below, extend_back=False, id_col=none, primary_below=none) %}
  {% if extend_back %}
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY {{ id_col }} ORDER BY GREATEST({{ utils_core.concat_affix(below) }})) = 1
        THEN {{ primary_below }}
      ELSE GREATEST({{ utils_core.concat_affix(below) }})
    END
  {% else %}
    GREATEST({{ utils_core.concat_affix(below) }})
  {% endif %}
{% endmacro %}
