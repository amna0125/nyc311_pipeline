{{ config(materialized='view') }}

/*
    Silver step 2 — derive measures, flags, and date parts.
    This is the conformed, analytics-ready grain (one row per request) that the
    gold dimensions and fact are built from.
*/

with cleaned as (
    select * from {{ ref('int_service_requests__cleaned') }}
)

select
    -- ── keys & raw attributes carried forward ─────────────────────────
    service_request_id,
    agency_code,
    agency_name,
    complaint_type,
    descriptor,
    descriptor_2,
    status,
    channel_type,
    borough,
    location_type,
    incident_address,
    street_name,
    city,
    landmark,
    facility_type,
    address_type,
    incident_zip,
    community_board,
    council_district,
    police_precinct,
    bbl,
    road_ramp,
    vehicle_type,
    taxi_company_borough,
    taxi_pick_up_location,
    has_valid_geo,
    latitude,
    longitude,
    x_coordinate_state_plane,
    y_coordinate_state_plane,
    resolution_description,

    -- ── timestamps ────────────────────────────────────────────────────
    created_at,
    closed_at,
    due_at,
    resolution_updated_at,

    -- ── derived measures ──────────────────────────────────────────────
    -- resolution time in hours; NULL when not closed or when the closed date
    -- precedes the created date (data-quality guard against negative durations).
    case
        when closed_at is not null and closed_at >= created_at
        then datediff('second', created_at, closed_at) / 3600.0
    end                                                        as resolution_time_hours,

    -- ── status / SLA flags ────────────────────────────────────────────
    (closed_at is not null or status = 'CLOSED')               as is_closed,
    (due_at is not null
        and coalesce(closed_at, current_timestamp::timestamp_ntz) > due_at)
                                                               as is_overdue,
    (due_at is not null and closed_at is not null and closed_at <= due_at)
                                                               as is_resolved_on_time,

    -- ── created_date parts (flattened inline; dim_date kept as a gap-free
    --    calendar spine for zero-activity days + BI time-intelligence) ────
    cast(created_at as date)                                   as created_date_day,
    year(created_at)                                           as created_year,
    quarter(created_at)                                        as created_quarter,
    month(created_at)                                          as created_month,
    monthname(created_at)                                      as created_month_name,
    dayofweek(created_at)                                      as created_day_of_week,
    dayname(created_at)                                        as created_day_name,
    weekofyear(created_at)                                     as created_iso_week,
    (dayofweek(created_at) in (0, 6))                          as created_is_weekend,
    hour(created_at)                                           as created_hour,

    -- ── audit ─────────────────────────────────────────────────────────
    ingested_at

from cleaned
