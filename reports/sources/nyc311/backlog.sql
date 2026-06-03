select
    borough,
    agency_code,
    total_requests,
    closed_requests,
    open_requests,
    open_overdue_requests,
    oldest_open_request_age_days
from NYC311.NYC_311_GOLD.mart_open_vs_closed_status
