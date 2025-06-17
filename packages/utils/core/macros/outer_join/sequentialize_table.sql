/* MACRO - Sequentialize Interval Table */
{% macro sequentialize_table(
    table,
    grouping_columns,
    interval_start_column,
    interval_end_column,
    validation_columns,
    pre_filter = '',
    value_columns = None,
    has_value_columns = True,
    flag_overlaps_column_name = None
  ) %}

-- Set the pre_filter argument like "WHERE valid_from < '2023-08-01'" to filter out intervals
-- that shouldn't be accounted for when sequentialising.

-- Set the value_columns argument to a list of columns that should be returned in the output.

{% if value_columns == None and has_value_columns %}
{% set value_columns = dbt_utils.star(from=table, except=grouping_columns+[interval_start_column,interval_end_column]).split(',') %}
{% if value_columns == ['/* no columns returned from star() macro */'] %}
{{ exceptions.raise_compiler_error("No columns returns from " ~ table ~ " model - are you sure it exists?") }}
{% endif %}
{% endif %}

  -- Get the interval start & end date columns
  WITH filtered_table AS (

    SELECT *
    FROM {{ table }}
    {{ pre_filter }}

), interval_points_wide AS (
    SELECT
      {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }},
      {{ interval_start_column }} AS interval_start,
      {{ interval_end_column }} AS interval_end
    FROM
      filtered_table
    WHERE {{ interval_start_column }} < COALESCE(
      {{ interval_end_column }}, {{ var('distant_future_timestamp') }}
    )
  ),
  -- Stack them into a single column and remove duplicates to get distinct intervals where values may differ
  interval_points_tall AS (
    SELECT
      DISTINCT {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }},
      interval_start AS interval_point
    FROM
      interval_points_wide
    UNION
    SELECT
      DISTINCT {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }},
      interval_end AS interval_point
    FROM
      interval_points_wide
  ),
  -- Convert to interval rows by taking the next rows interval point value as the end timestamp of the preceding row
  interval_points AS (
    SELECT
      {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }},
      interval_point AS interval_start,
      LEAD(interval_point, 1) OVER by_group_asc AS interval_end
    FROM
      interval_points_tall
    QUALIFY RANK() over by_group_desc != 1
    WINDOW
      by_group_asc AS (
        PARTITION BY {{ utils_core.concat_affix(grouping_columns, ',') }} ORDER BY interval_point ASC NULLS LAST),
      by_group_desc AS (
        PARTITION BY {{ utils_core.concat_affix(grouping_columns, ',') }} ORDER BY interval_point DESC NULLS FIRST
      )
  ) -- Join the original table back onto your interval dataset and use a validation function to drop duplicates.
  -- In this example we will use the latest id to filter on most recent agreements where there are overlaps.

SELECT
  {{ utils_core.concat_affix(
    grouping_columns,
    delim = ',',
    prefix = 'ip.'
  ) }},
  ip.interval_start AS {{ interval_start_column }},
  ip.interval_end AS {{ interval_end_column }}
  {% for value_col in value_columns %}
    , MAX_BY(
      tbl.{{ value_col }},(
      {{ utils_core.concat_affix(
        validation_columns,
        delim = ','
      ) }})
    ) AS {{ value_col }}
  {% endfor %}

  {% if flag_overlaps_column_name %},
    CASE
      WHEN COUNT(
        ip.interval_start
      ) > 1 THEN TRUE
      ELSE FALSE
    END AS {{ flag_overlaps_column_name }}
  {% endif %}
FROM
  interval_points ip
  LEFT JOIN filtered_table tbl
  ON {% for grouping_col in grouping_columns %}
    tbl.{{ grouping_col }} = ip.{{ grouping_col }}

    {% if not loop.last %}
      AND
    {% endif %}
  {% endfor %}
  AND {{ utils_core.intervals_overlap(
    below=["tbl." + interval_start_column, "ip.interval_start"],
    above=[
      "COALESCE(tbl." + interval_end_column + ", " + var("distant_future_timestamp") + ")",
      "COALESCE(ip.interval_end, " + var("distant_future_timestamp") + ")",
    ]
  ) }}


WHERE
  {{ utils_core.concat_affix(
    grouping_columns,
    delim = ' AND ',
    prefix = 'tbl.',
    suffix = ' IS NOT NULL'
  ) }}
  AND tbl.{{ interval_start_column }} < COALESCE(tbl.{{ interval_end_column }}, {{var('distant_future_timestamp')}})
GROUP BY
  {{ utils_core.concat_affix(
    grouping_columns,
    delim = ',',
    prefix = 'ip.'
  ) }},
  ip.interval_start,
  ip.interval_end
ORDER BY
  {{ utils_core.concat_affix(
    grouping_columns,
    delim = ',',
    prefix = 'ip.'
  ) }},
  ip.interval_start,
  ip.interval_end
{% endmacro %}