with
    dummy_data as (
        select
            1 as account_id,
            'Business' as string_val,
            10 as int_val,
            TRUE as bool_val,
            timestamp('2020-01-01 00:00:00') as valid_from,
            timestamp('2020-01-05 00:00:00') as valid_to
        union all
        select
            1 as account_id,
            'Business' as string_val,
            10 as int_val,
            TRUE as bool_val,
            timestamp('2020-01-05 00:00:00') as valid_from,
            timestamp('2020-01-10 00:00:00') as valid_to
        UNION ALL
        select
            2 as account_id,
            'Big' as string_val,
            16 as int_val,
            TRUE as bool_val,
            timestamp('2020-01-01 00:00:00') as valid_from,
            timestamp('2020-01-05 00:00:00') as valid_to
        union all
        select
            2 as account_id,
            'Small' as string_val,
            7 as int_val,
            FALSE as bool_val,
            timestamp('2020-01-05 00:00:00') as valid_from,
            timestamp('2020-01-15 00:00:00') as valid_to
    ),
flattened AS (
{{ utils_core.flatten_sequential_rows_mixed_datatypes(
        table='dummy_data'
        , grouping_columns=['account_id']
        , interval_start_column='valid_from'
        , interval_end_column='valid_to'
        , value_columns=['string_val', 'int_val', 'bool_val']
    )
  }}
)

SELECT *
FROM flattened