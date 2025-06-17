WITH cte1 AS (
    SELECT
        1 as id,
        'Alice' as name
),
cte2 AS (
    SELECT
        2 as id,
        'Bob' as name
)

{{
    utils_core.union_cte(
        cte1='cte1',
        cte2='cte2',
        union_type='ALL',
        column_names=['id', 'name']
        )
}}