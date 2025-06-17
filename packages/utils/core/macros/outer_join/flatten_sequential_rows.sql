/* MACRO - Flatten Sequential Interval Rows in Table */
{% macro flatten_sequential_rows(
    table,
    grouping_columns,
    interval_start_column,
    interval_end_column,
    value_columns=[],
    exclude_value_columns=[]
  ) %}

{% if value_columns != [] and exclude_value_columns != [] %}
    {{ exceptions.raise_compiler_error("You cannot specify both 'value_columns' and 'exclude_value_columns' in the same call to 'flatten_sequential_rows'") }}
{% endif %}

{% if value_columns == [] %}
    {% set value_columns = dbt_utils.star(from=table, except=grouping_columns+[interval_start_column,interval_end_column] + exclude_value_columns).split(',') %}
{% endif %}

/*
 This macro is used to combine sequential interval rows where values match.
 For reference see https://bertwagner.com/posts/gaps-and-islands/

 'exclude_value_columns' should be a list of all columns where the value is unique to that row but
 are not important when grouping rows - e.g. row updated timestamps - these will be omitted from
 the resulting combined dataset.)
 */
SELECT {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }},
       {{ utils_core.concat_affix(
        value_columns,
        ','
      ) }},
       MIN(StartDate) AS {{ interval_start_column }},
       MAX(EndDate)   AS {{ interval_end_column }}
FROM (
         SELECT *,
                SUM(CASE
                        WHEN Groups.PreviousEndDate >= StartDate AND
                             Groups.value_array = ARRAY({{ utils_core.concat_affix(
        value_columns,
        ','
      ) }})
                            THEN 0
                        ELSE 1 END) OVER (PARTITION BY {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }} ORDER BY Groups.RN) AS IslandId
         FROM (
                  SELECT tbl.*,
                         ROW_NUMBER()
                                 OVER (PARTITION BY {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }} ORDER BY {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }}, {{ interval_start_column }},{{ interval_end_column }})                                                            AS RN,
                         {{ interval_start_column }}                                                                          as StartDate,
                         {{ interval_end_column }}                                                                            as EndDate,
                         LAG({{ interval_end_column }}, 1)
                             OVER (PARTITION BY {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }} ORDER BY {{ interval_start_column }}, {{ interval_end_column }}) AS PreviousEndDate,
                         LAG(ARRAY({{ utils_core.concat_affix(
        value_columns,
        ','
      ) }}), 1)
                             OVER (PARTITION BY {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }} ORDER BY {{ interval_start_column }}, {{ interval_end_column }}) AS value_array
                  FROM {{ table }} tbl
                  ORDER BY {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }}, {{ interval_start_column }}, {{ interval_end_column }}
              ) Groups
         ORDER BY {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }}, {{ interval_start_column }}, {{ interval_end_column }}
     ) Islands
GROUP BY {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }},
         {{ utils_core.concat_affix(
        value_columns,
        ','
      ) }},
         IslandId
ORDER BY {{ utils_core.concat_affix(
        grouping_columns,
        ','
      ) }},
         {{ interval_start_column }}
{% endmacro %}