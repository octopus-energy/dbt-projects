/* MACRO - Merge Sequential Interval tables

WARNING: if `join_type` != 'outer', only
intervals where there is a value in left table will be returned
*/
{% macro merge_sequential_intervals(
    left_table,
    merging_columns,
    left_interval_start_column,
    left_interval_end_column,
    left_columns,
    right_table,
    right_interval_start_column,
    right_interval_end_column,
    right_columns,
    join_type='left',
    common_column_strategy='both',
    left_table_common_prefix='left_',
    right_table_common_prefix='right_',
    left_incremental_timestamp=None,
    right_incremental_timestamp=None,
    merged_incremental_timestamp=None
  ) %}

  {% if join_type not in ('left', 'outer') %}
    {{ exceptions.raise_compiler_error("`join_type` argument for merge_sequential_intervals macro must be one of ['left', 'outer'] Got: '" ~ join_type ~"'.'") }}
  {% endif %}

  {% set left_value_columns_filtered = [] %}
  {% for col in left_columns %}
    {% if (col in merging_columns or col in [left_interval_start_column, left_interval_end_column]) %}
    {% elif col in right_columns %}
        {% if common_column_strategy == 'both' %}
            {% do left_value_columns_filtered.append(col + ' AS ' + left_table_common_prefix + col) %}
        {% elif common_column_strategy == 'left' %}
            {% do left_value_columns_filtered.append(col) %}
        {% elif common_column_strategy == 'right' %}
        {% else %}
            {{ exceptions.raise_compiler_error("`common_column_strategy` argument for merge_sequential_intervals macro must be one of ['both', 'left', 'right'] Got: '" ~ common_column_strategy ~"'.'") }}
        {% endif %}
    {% else %}
        {% do left_value_columns_filtered.append(col) %}
    {% endif %}
  {% endfor %}

  {% set right_value_columns_filtered = [] %}
  {% for col in right_columns %}
    {% if (col in merging_columns or col in [right_interval_start_column, right_interval_end_column]) %}
    {% elif col in left_columns %}
        {% if common_column_strategy == 'both' %}
            {% do right_value_columns_filtered.append(col + ' AS ' + right_table_common_prefix + col) %}
        {% elif common_column_strategy == 'left' %}
        {% elif common_column_strategy == 'right' %}
            {% do right_value_columns_filtered.append(col) %}
        {% else %}
            {{ exceptions.raise_compiler_error("`common_column_strategy` argument for merge_sequential_intervals macro must be one of ['both', 'left', 'right'] Got: '" ~ common_column_strategy ~"'.'") }}
        {% endif %}
    {% else %}
      {% do right_value_columns_filtered.append(col) %}
    {% endif %}
  {% endfor %}

    -- To get a merged list of intervals, we need to build up the cross section of all
    -- of the intervals from each of the tables (along with the value of the columns we
    -- will use to join during each interval). Once we have these intervals, we know
    -- that nothing changes within each of them, so we can use them to build our merged
    -- sequence.
    --
    -- To build this cross section, we:
    -- * Get all of the intervals from the left and right tables, and combine them into
    --   a single interval_points_wide table, which will contain overlaps
    -- * Convert this into a table that just contains the end points of each interval
    --   (interval_points_tall)
    -- * Create the final merged_intervals table by grouping by the values of the merge
    --   columns, ordering the interval endpoints in each group and then iterating
    --   pairwise to create the merged intervals
  WITH interval_points_wide AS (
    SELECT
      {{ utils_core.concat_affix(
        merging_columns,
        ','
      ) }},
      {{ left_interval_start_column }} AS interval_start,
      {{ left_interval_end_column }} AS interval_end
    FROM
      {{ left_table }}
    UNION ALL
    SELECT
      {{ utils_core.concat_affix(
        merging_columns,
        ','
      ) }},
      {{ right_interval_start_column }} AS interval_start,
      {{ right_interval_end_column }} AS interval_end
    FROM
      {{ right_table }}
  ),
  interval_points_tall AS (
    SELECT
      DISTINCT *
    FROM
      (
        SELECT
          DISTINCT {{ utils_core.concat_affix(
            merging_columns,
            ','
          ) }},
          interval_start AS interval_point
        FROM
          interval_points_wide
        UNION ALL
        SELECT
          DISTINCT {{ utils_core.concat_affix(
            merging_columns,
            ','
          ) }},
          interval_end AS interval_point
        FROM
          interval_points_wide
      )
  ),
  merged_intervals AS (
    SELECT
      {{ utils_core.concat_affix(
        merging_columns,
        ','
      ) }},
      interval_point AS interval_start,
      LEAD(
        interval_point,
        1
      ) OVER (
        PARTITION BY {{ utils_core.concat_affix(
          merging_columns,
          ','
        ) }}
        ORDER BY
          interval_point
      ) AS interval_end
    FROM
      interval_points_tall
  )
-- Now that we have our merged list of intervals, we can join back onto the original
-- tables to get the values of the combined sequence for each merged interval. We use
-- LEFT joins to allow us to join tables where there is not always a value for both
-- tables for every merged interval (for example, if we want to join over a reverse
-- foreign key relationship).
, final AS (
SELECT
  {% for merging_col in merging_columns %}
  COALESCE(_left.{{ merging_col }}, _right.{{ merging_col }}) AS {{ merging_col }},
  {% endfor %}
  mi.interval_start,
  mi.interval_end
  {% for col in left_value_columns_filtered %}
    , _left.{{col}}
  {% endfor %}
  {% for col in right_value_columns_filtered %}
    , _right.{{col}}
  {% endfor %}
FROM
  merged_intervals mi
  LEFT JOIN {{ left_table }}
  _left
  ON {% for merging_col in merging_columns %}
    _left.{{ merging_col }} = mi.{{ merging_col }}

    {% if not loop.last %}
      AND
    {% endif %}
  {% endfor %}
  AND _left.{{ left_interval_start_column }} < COALESCE(mi.interval_end, {{ var('distant_future_timestamp') }})
  AND COALESCE(
    _left.{{ left_interval_end_column }},
    {{ var('distant_future_timestamp') }}
  ) > mi.interval_start
  LEFT JOIN {{ right_table }}
  _right
  ON {% for merging_col in merging_columns %}
    _right.{{ merging_col }} = mi.{{ merging_col }}

    {% if not loop.last %}
      AND
    {% endif %}
  {% endfor %}
  AND _right.{{ right_interval_start_column }} < COALESCE(mi.interval_end, {{ var('distant_future_timestamp') }})
  AND COALESCE(
    _right.{{ right_interval_end_column }},
    {{ var('distant_future_timestamp') }}
  ) > mi.interval_start


WHERE
  {% if join_type == 'left' %}
    {% for merging_col in merging_columns %}
      _left.{{ merging_col }} IS NOT NULL {% if not loop.last %} AND {% endif %}
    {% endfor %}
  {% else %}
    (
      (
      {% for merging_col in merging_columns %}
        _left.{{ merging_col }} IS NOT NULL {% if not loop.last %} AND {% endif %}
      {% endfor %}
      )
    OR
      (
      {% for merging_col in merging_columns %}
        _right.{{ merging_col }} IS NOT NULL {% if not loop.last %} AND {% endif %}
      {% endfor %}
      )
    )
  {% endif %}
  {% if is_incremental() %}
     -- this filter will only be applied on an incremental run
AND
(
    {{ left_incremental_timestamp }} > (select max({{ merged_incremental_timestamp }}) from {{ this }})
OR  {{ right_incremental_timestamp }} > (select max({{ merged_incremental_timestamp }}) from {{ this }})
)
    {% endif %}
)

SELECT
  *
FROM final
{% endmacro %}
