/* MACRO - Flatten Sequential Interval Rows in Table */
{% macro flatten_sequential_rows_no_value_cols(
  table,
  grouping_columns,
  interval_start_column,
  interval_end_column
  ) %}


  /*
   This macro is used to combine sequential interval rows where values match.
   For reference see https://bertwagner.com/posts/gaps-and-islands/

   'exclude_value_columns' should be a list of all columns where the value is unique to that row but
   are not important when grouping rows - e.g. row updated timestamps - these will be omitted from
   the resulting combined dataset.)
   */
SELECT
  {{ utils_core.concat_affix(grouping_columns, ',') }},
  MIN(StartDate) AS {{ interval_start_column }},
  MAX(EndDate) AS {{ interval_end_column }}
FROM
  (
    SELECT
      *,
      SUM(
        CASE
          WHEN Groups.PreviousEndDate >= StartDate THEN 0
          ELSE 1
        END
      ) OVER (
        PARTITION BY {{ utils_core.concat_affix(grouping_columns, ',') }}
        ORDER BY
          Groups.RN
      ) AS IslandId
    FROM
      (
        SELECT
          tbl.*,
          ROW_NUMBER() OVER (
            PARTITION BY {{ utils_core.concat_affix(grouping_columns, ',') }}
            ORDER BY
              {{ utils_core.concat_affix(grouping_columns, ',') }},
              {{ interval_start_column }},
              {{ interval_end_column }}
          ) AS RN,
          {{ interval_start_column }} as StartDate,
          {{ interval_end_column }} as EndDate,
          LAG({{ interval_end_column }}, 1) OVER (
            PARTITION BY {{ utils_core.concat_affix(grouping_columns, ',') }}
            ORDER BY
              {{ interval_start_column }},
              {{ interval_end_column }}
          ) AS PreviousEndDate
        FROM
          {{ table }} tbl
        ORDER BY
          {{ utils_core.concat_affix(grouping_columns, ',') }},
          {{ interval_start_column }},
          {{ interval_end_column }}
      ) Groups
    ORDER BY
      {{ utils_core.concat_affix(grouping_columns, ',') }},
      {{ interval_start_column }},
      {{ interval_end_column }}
  ) Islands
GROUP BY
  {{ utils_core.concat_affix(grouping_columns, ',') }},
  IslandId
ORDER BY
  {{ utils_core.concat_affix(grouping_columns, ',') }},
  {{ interval_start_column }}

{% endmacro %}