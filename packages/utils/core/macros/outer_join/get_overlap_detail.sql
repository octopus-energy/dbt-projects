{% macro get_overlap_detail(
    table,
    grouping_columns,
    interval_start_column,
    interval_end_column,
    validation_columns
  ) %}
  {% set flag_overlaps_column_name = "`__get_overlap_detail_internal__is_overlapping`" %}
  WITH sequenced_table AS (
    {{ sequentialize_table(
      table = table ,
      grouping_columns = grouping_columns,
      interval_start_column = interval_start_column,
      interval_end_column = interval_end_column,
      validation_columns = validation_columns,
      flag_overlaps_column_name = flag_overlaps_column_name
    ) }}
  ),
  overlaps AS (
    SELECT
      {{ concat_affix(
        grouping_columns,
        ','
      ) }},
      {{ interval_start_column }} AS overlap_start,
      {{ interval_end_column }} AS overlap_end
    FROM
      sequenced_table
    WHERE
      {{ flag_overlaps_column_name }}
  ),
  overlaps_detail AS (
    SELECT
      orig.*,
      overlaps.overlap_start,
      overlaps.overlap_end
    FROM
      overlaps
      LEFT JOIN {{ table }} orig
      ON {% for grouping_col in grouping_columns %}
        overlaps.{{ grouping_col }} = orig.{{ grouping_col }}

        {% if not loop.last %}
          AND
        {% endif %}
      {% endfor %}
      AND overlaps.overlap_start < COALESCE(
        orig.{{ interval_end_column }},
        '9999-12-31'
      )
      AND COALESCE(
        overlaps.overlap_end,
        '9999-12-31'
      ) > orig.{{ interval_start_column }}
  )
SELECT
  *
FROM
  overlaps_detail
WHERE {% for grouping_col in grouping_columns %}
       {{ grouping_col }} IS NOT NULL

        {% if not loop.last %}
          OR
        {% endif %}
      {% endfor %}
{% endmacro %}
