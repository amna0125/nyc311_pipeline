select
    created_date_day,
    borough,
    agency_code,
    complaint_type,
    total_requests,
    closed_requests,
    open_requests,
    avg_resolution_hours
from NYC311.NYC_311_GOLD.mart_daily_requests_by_borough_agency
