{% if execute %}

{% set groups = run_query("SHOW GROUPS").columns[0].values() %}

select '{{ groups[0] }}' as group
{% for group in groups[1:] %}
union all
select '{{ group }}'
{% endfor %}

{% else %}

select 'dummy_value' as group

{% endif %}
