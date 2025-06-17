WITH cte1 AS (
    SELECT
        1 as id,
        'Alice' as name
),
cte2 AS (
    SELECT
        1 as id,
        'Alice' as name
)

{{
    utils_core.union_cte(
        cte1='cte1',
        cte2='cte2',
        union_type='DISTINCT',
        column_names=['id', 'name']
        )
}}