{% docs intervals_overlap %}
This macro checks that several intervals overlap, returning a SQL expression evaluating to a
boolean.
{% enddocs %}

{% docs merged_interval_above %}
This macro returns the upper end of the overlapping interval that is covered by _all_ of the input
intervals.
{% enddocs %}

{% docs merged_interval_below %}
This macro returns the lower end of the overlapping interval that is covered by _all_ of the input
intervals.
{% enddocs %}
