{% macro ms_int_to_time_column(ms_int_column, reference_00_ms_int=-1462009856, time_format='HH:mm:ss') %}
    CAST(
        CAST(
            ( date_format( CAST(unix_millis(TIMESTAMP('1970-01-01 00:00:00')) +
                ({{ ms_int_column }} - ({{ reference_00_ms_int }}))/1000 AS timestamp)
                , '{{ time_format }}')
            ) AS string
        ) AS INTERVAL HOUR TO SECOND
    )
{% endmacro %}