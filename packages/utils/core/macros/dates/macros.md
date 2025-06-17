{% docs date_between %}
This macro checks that a given date is contained within the interval defined by the dates `below`
and `above`, where NULL dates for `below` and `above` are considered to represent dates in the
distant past and future respectively. The interval is assumed to be left-closed (and right-open),
ie below <= date < above.
{% enddocs %}
{% docs local_midnight_timestamp %}
This macro takes a UTC timestamp, converts it to another timezone (the default is set to the local timezone of the operating region),
then taking the date of this updated timestamp before casting back to UTC timestamp at a 00:00:00 time. 
The purpose of this macro is for instances when joining two timestamps, one of which has been 
artificially cast from a date in the staging layer to a 00:00:00 time and therefore negating the time 
element of join.
{% enddocs %}
{% docs date_spine %}
Create a "date spine", ie a SQL table with one row per timestamp, according to the frequency
defined in the `datepart` argument, between the `start_date` and `end_date`.
{% enddocs %}
