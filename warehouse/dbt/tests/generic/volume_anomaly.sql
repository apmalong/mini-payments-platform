{#- Fails when the last complete day's row count is more than `threshold` standard deviations
    from the mean of the `lookback_days` before it. Today is excluded because it's still filling. -#}
{% test volume_anomaly(model, date_column, lookback_days=14, threshold=3) %}

with daily as (
    select {{ date_column }} as day, count(*) as row_count
    from {{ model }}
    where {{ date_column }} < current_date
    group by 1
),

checked as (
    select
        day,
        row_count,
        avg(row_count) over baseline_window as baseline_mean,
        stddev_samp(row_count) over baseline_window as baseline_stddev
    from daily
    window baseline_window as (order by day rows between {{ lookback_days }} preceding and 1 preceding)
)

select day, row_count, round(baseline_mean, 1) as baseline_mean, round(baseline_stddev, 1) as baseline_stddev
from checked
where day = (select max(day) from daily)
  and baseline_stddev > 0
  and abs(row_count - baseline_mean) > {{ threshold }} * baseline_stddev

{% endtest %}
