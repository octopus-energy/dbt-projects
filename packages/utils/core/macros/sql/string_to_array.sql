{% macro string_to_array(value, ltrim='["', rtrim='"]', separator='","', empty_value='[]') %}
CASE
  WHEN {{ value }} = '{{ empty_value }}' THEN ARRAY()
  ELSE SPLIT(LTRIM('{{ ltrim }}', RTRIM('{{ rtrim }}', {{ value }})), '{{ separator }}')
END
{% endmacro %}
