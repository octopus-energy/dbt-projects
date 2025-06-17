-- depends_on: {{ ref('dim_databricks_user') }}
-- depends_on: {{ ref('dim_databricks_group') }}
{% set tables = run_query("SELECT * FROM " ~ env_var('PROD_CATALOG' ) ~ ".information_schema.tables WHERE table_schema = 'databricks' and table_name = 'dim_databricks_user'") %}

{% if execute and tables|length != 0 %}

    {% set users = run_query("SELECT user FROM " ~ ref('dim_databricks_user')).columns[0].values() %}

    {% set users_in_groups_query = [] %}

    {% for user in users %}
        {% set group_rows = run_query("SHOW GROUPS WITH USER `" ~ user ~ "`") %}

        {% for row in group_rows %}

            {% set query_row = "SELECT '" ~ user ~ "' AS user, '" ~ row['name'] ~ "' AS group, " ~ row['directGroup'] ~ " AS is_direct_group" %}
            {% do users_in_groups_query.append(query_row) %}
        {% endfor %}
    {% endfor %}

    WITH users_in_groups AS (
        {% for query in users_in_groups_query %}
            {% if not loop.first %} UNION ALL {% endif %}
            {{ query }}
        {% endfor %}
    ),

    -- Step 2: Create a CTE to fetch all users with the group 'users' added manually
    all_users AS (
        SELECT
            user,
            'users' AS group
        FROM
            {{ ref('dim_databricks_user') }}
    ),

    -- Step 3: Full outer join of all users and groups
    all_users_groups AS (
        SELECT
            COALESCE(g.group, u.group) AS group,
            COALESCE(u.user, g.user) AS user,
            g.is_direct_group
        FROM
            users_in_groups g
        FULL OUTER JOIN
            all_users u
        ON
            g.group = u.group AND g.user = u.user
    ),

    -- Step 4: Filtering and final processing
    filtered_users AS (
        SELECT
            *,
            SHA2(CONCAT_WS('||', COALESCE(all_users_groups.group, ''), COALESCE(all_users_groups.user, ''), COALESCE(all_users_groups.is_direct_group, NULL)), 256) AS group_user_id
        FROM
            all_users_groups
        WHERE
            (user IS NOT NULL AND group = 'users') OR (group != 'users')
    )

    -- Final output
    SELECT
        group,
        user AS user_name,
        is_direct_group,
        group_user_id
    FROM
        filtered_users

{% else %}

select 'dummy_value' as user_name, 'dummy_value' as group, 'dummy_value' as group_user_id

{% endif %}