{% if execute %}

{% set users = run_query("SHOW USERS").columns[0].values() %}

select '{{ users[0] }}' as user
{% for user in users[1:] %}
union all
select '{{ user }}'
{% endfor %}

{% else %}

select 'dummy_value' as user

{% endif %}
