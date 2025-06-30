{% if execute %}

{% set users = run_query("SHOW USERS").columns[0].values() %}

SELECT *
FROM (
    VALUES
    {% for user in users %}
        ('{{ user }}'){% if not loop.last %},{% endif %}
    {% endfor %}
) AS user_table(user)

{% else %}

SELECT 'dummy_value' AS user

{% endif %}
