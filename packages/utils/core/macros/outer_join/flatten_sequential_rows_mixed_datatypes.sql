/* MACRO - Flatten Sequential Interval Rows in Table where value columns can have mixed datatypes */
{% macro flatten_sequential_rows_mixed_datatypes(
    table,
    grouping_columns,
    interval_start_column,
    interval_end_column,
    value_columns=[],
    exclude_value_columns=[],
    preset_filter=''
  ) %}

{% if value_columns != [] and exclude_value_columns != [] %}
    {{ exceptions.raise_compiler_error("You cannot specify both 'value_columns' and 'exclude_value_columns' in the same call to 'flatten_sequential_rows_mixed_datatypes'") }}
{% endif %}

{% if value_columns == [] %}
    {% set value_columns = dbt_utils.star(from=table, except=grouping_columns+[interval_start_column,interval_end_column] + exclude_value_columns).split(',') %}
{% endif %}

{% set interval_cols = [interval_start_column, interval_end_column] %}
/*
 This macro is used to combine sequential interval rows where values match.
 For reference see https://bertwagner.com/posts/gaps-and-islands/
 'exclude_value_columns' should be a list of all columns where the value is unique to that row but
 are not important when grouping rows - e.g. row updated timestamps - these will be omitted from
 the resulting combined dataset.)
 */

WITH table_to_flatten AS (
  SELECT
    {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }}
    , {{ utils_core.concat_affix(
        value_columns,
        ','
      ) }}
    , {{ interval_start_column }} AS {{ interval_start_column }}
    , {{ interval_end_column }} AS {{ interval_end_column }}
  FROM {{ table }}
  {{ preset_filter }}
)

-- Combine sequential intervals where values match (https://bertwagner.com/posts/gaps-and-islands/)
, config_groups AS (
  SELECT
    *
    -- Create a row number column based on the sequence of start and end dates
    -- and bring the previous row's end date to the current row
    , ROW_NUMBER() OVER (
      PARTITION BY {{ utils_core.concat_affix(
          grouping_columns,
          ','
        ) }}
      , {{ utils_core.concat_affix(
          value_columns,
          ','
        ) }}
      ORDER BY {{ interval_cols | join(', ') }}
    ) AS config_rn
    , LAG({{ interval_end_column }}
    ) OVER (
      PARTITION BY {{ utils_core.concat_affix(
          grouping_columns,
          ','
        ) }}
      , {{ utils_core.concat_affix(
          value_columns,
          ','
        ) }}
      ORDER BY {{ interval_cols | join(', ') }}
    ) AS prev_config_valid_to
  FROM table_to_flatten
)

-- Find groups of continuous configs (islands)
, interval_islands AS (
  SELECT
    *
    -- Show which island number the current row belongs to
    , SUM(
      CASE
        WHEN prev_config_valid_to >= {{ interval_start_column }} THEN 0 ELSE 1
      END
    ) OVER (
      PARTITION BY {{ utils_core.concat_affix(
          grouping_columns,
          ','
        ) }}
      , {{ utils_core.concat_affix(
          value_columns,
          ','
        ) }}
      ORDER BY config_rn ASC
    ) AS island_id
  FROM config_groups
)

-- Aggregate rows to return the minimum and maximum start and end dates of each island
, collapsed_intervals AS (
  SELECT
    {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }}
    , {{ utils_core.concat_affix(
        value_columns,
        ','
      ) }}
    , MIN({{ interval_start_column }}) AS {{ interval_start_column }}
    , MAX({{ interval_end_column }}) AS {{ interval_end_column }}
  FROM interval_islands
  GROUP BY
    {{ utils_core.concat_affix(
      grouping_columns,
      ','
    ) }}
  , {{ utils_core.concat_affix(
      value_columns,
      ','
    ) }}
    , island_id
)

SELECT
  *
FROM collapsed_intervals

 {% endmacro %}