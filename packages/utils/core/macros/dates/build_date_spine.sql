{%- macro build_date_spine(start_date, end_date) %}

select
    DATE(date_day) as date_day,
    {#- /* Extract day of week from date */ #}
    date_format(date_day, 'E') as day_of_week,
    {#- /* Determine whether date is a weekend (Sat-Sun) */ #}
    date_format(date_day, 'E') in ('Sat','Sun') as is_weekend
from
({{
    dbt_utils.date_spine(
        datepart="day",
        start_date=start_date,
        end_date=end_date
    )
}})

{%- endmacro %}
