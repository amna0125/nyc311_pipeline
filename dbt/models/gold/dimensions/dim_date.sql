{{ config(materialized='table') }}

/*
    Date dimension built from a generated spine, covering the dataset history
    window through today. date_sk is an integer YYYYMMDD for cheap, readable joins.
*/

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('request_min_date') ~ "' as date)",
        end_date="dateadd('day', 1, current_date)"
    ) }}
),

dates as (
    select cast(date_day as date) as date_day
    from spine
)

select
    cast(to_char(date_day, 'YYYYMMDD') as integer)  as date_sk,
    date_day,
    year(date_day)                                   as year,
    quarter(date_day)                                as quarter,
    month(date_day)                                  as month,
    monthname(date_day)                              as month_name,
    day(date_day)                                    as day_of_month,
    dayofweek(date_day)                              as day_of_week,
    dayname(date_day)                                as day_name,
    weekofyear(date_day)                             as iso_week,
    (dayofweek(date_day) in (0, 6))                  as is_weekend
from dates
