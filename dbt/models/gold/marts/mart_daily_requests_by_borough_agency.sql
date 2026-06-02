{{ config(materialized='table') }}

/*
    Daily request volume and closure rate by borough, agency, and complaint type.
    Reads directly from the wide fact — the descriptive attributes are already
    columns there, so no dimension joins are needed.
*/

select
    created_date_sk,
    created_date_day,
    borough,
    agency_code,
    complaint_type,
    count(*)                                as total_requests,
    sum(iff(is_closed, 1, 0))               as closed_requests,
    sum(iff(is_closed, 0, 1))               as open_requests,
    round(avg(resolution_time_hours), 2)    as avg_resolution_hours
from {{ ref('fct_service_requests') }}
group by 1, 2, 3, 4, 5
