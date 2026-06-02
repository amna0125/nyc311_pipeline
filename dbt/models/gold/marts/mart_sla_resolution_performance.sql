{{ config(materialized='table') }}

/*
    SLA / resolution performance by agency and complaint type, per created month.
    Reads directly from the wide fact. Closed requests drive the resolution-time
    stats; on-time / overdue rates use the SLA flags from silver.
*/

select
    date_trunc('month', created_at)::date            as created_month,
    agency_code,
    complaint_type,
    count(*)                                          as total_requests,
    sum(iff(is_closed, 1, 0))                         as closed_requests,
    round(avg(resolution_time_hours), 2)              as avg_resolution_hours,
    round(median(resolution_time_hours), 2)           as median_resolution_hours,
    round(percentile_cont(0.90) within group (
        order by resolution_time_hours), 2)           as p90_resolution_hours,
    round(100.0 * sum(iff(is_resolved_on_time, 1, 0))
          / nullif(sum(iff(is_closed, 1, 0)), 0), 1)  as pct_resolved_on_time,
    round(100.0 * sum(iff(is_overdue, 1, 0))
          / nullif(count(*), 0), 1)                   as pct_overdue
from {{ ref('fct_service_requests') }}
group by 1, 2, 3
