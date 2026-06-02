{{
    config(
        materialized='incremental',
        unique_key='service_request_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        cluster_by=['created_date_sk']
    )
}}

/*
    Wide fact at one row per service request (degenerate business key
    service_request_id). This is a "one-big-table" fact: descriptive attributes
    (borough, agency, complaint type, status, channel, location) live here as
    degenerate dimensions rather than in separate dim tables — they are plain
    labels with no extra attributes, so a star join would add nothing.

    The only conformed dimension that earns its own table is dim_date, joined via
    created_date_sk (a gap-free calendar with weekend/month/quarter attributes).

    Incremental (merge): filtering on ingested_at captures both NEW requests and
    MUTATED ones (status flips, late closed_date via dlt's overlap reload); the
    merge updates the existing row in place.
*/

with src as (
    select * from {{ ref('int_service_requests__enriched') }}

    {% if is_incremental() %}
    where ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
    {% endif %}
)

select
    -- ── business key + date dimension FK ──────────────────────────────
    service_request_id,
    cast(to_char(created_date_day, 'YYYYMMDD') as integer)  as created_date_sk,

    -- ── degenerate dimensions (descriptive attributes) ────────────────
    agency_code,
    agency_name,
    complaint_type,
    descriptor,
    status,
    (status in ('CLOSED', 'CANCELED', 'CANCELLED'))         as is_terminal_status,
    channel_type,
    borough,
    incident_zip,
    community_board,
    council_district,
    police_precinct,
    location_type,

    -- ── geo ───────────────────────────────────────────────────────────
    has_valid_geo,
    latitude,
    longitude,

    -- ── timestamps + flattened date attributes ───────────────────────
    created_at,
    created_date_day,
    created_year,
    created_quarter,
    created_month,
    created_month_name,
    created_day_of_week,
    created_day_name,
    created_iso_week,
    created_is_weekend,
    created_hour,
    closed_at,
    due_at,

    -- ── measures ──────────────────────────────────────────────────────
    resolution_time_hours,
    is_closed,
    is_overdue,
    is_resolved_on_time,
    1                                                       as request_count,

    -- ── audit ─────────────────────────────────────────────────────────
    ingested_at

from src
