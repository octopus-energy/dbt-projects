with
    dummy_table_one as (
        select
            1 as account_id,
            'Business' as left_val,
            timestamp('2020-01-01 00:00:00') as valid_from,
            timestamp('2020-01-05 00:00:00') as valid_to
        union all
        select
            1 as account_id,
            'Domestic' as left_val,
            timestamp('2020-01-05 00:00:00') as valid_from,
            timestamp('2020-01-10 00:00:00') as valid_to
    ),
    dummy_table_two as (
        select
            1 as account_id,
            'Big' as right_val,
            timestamp('2020-01-01 00:00:00') as valid_from,
            timestamp('2020-01-05 00:00:00') as valid_to
        union all
        select
            1 as account_id,
            'Small' as right_val,
            timestamp('2020-01-05 00:00:00') as valid_from,
            timestamp('2020-01-15 00:00:00') as valid_to
    ),

final as (
{{
    utils_core.merge_sequential_intervals(
        left_table='dummy_table_one',
        merging_columns=['account_id'],
        left_interval_start_column='valid_from',
        left_interval_end_column='valid_to',
        left_columns=['left_val'],
        right_table='dummy_table_two',
        right_interval_start_column='valid_from',
        right_interval_end_column='valid_to',
        right_columns=['right_val'],
        join_type='left'
    )

}}

)
SELECT
    *
FROM
    final