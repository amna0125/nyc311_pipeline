{{ config(materialized='table') }}

/*
    Current backlog snapshot by borough and agency: open vs closed counts, the
    overdue backlog, and the age of the oldest still-open request. Reads directly
    from the wide fact.
*/

select
    borough,
    agency_code,
    count(*)                                            as total_requests,
    sum(iff(is_closed, 1, 0))                           as closed_requests,
    sum(iff(is_closed, 0, 1))                           as open_requests,
    sum(iff(not is_closed and is_overdue, 1, 0))        as open_overdue_requests,
    max(iff(is_closed, null,
        datediff('day', created_at, current_timestamp::timestamp_ntz)))
                                                        as oldest_open_request_age_days
from {{ ref('fct_service_requests') }}
group by 1, 2
