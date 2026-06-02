{{ config(materialized='view') }}

/*
    Silver step 1 — clean & standardize.
    Scrub sentinel "missing" values to NULL, standardize controlled vocabularies
    (borough, status, channel) to canonical UPPER forms, and enforce structural
    formats (ZIP, geo bounding box). No derived measures yet (that is __enriched).
*/

with bronze as (
    select * from {{ ref('stg_nyc311__service_requests') }}
)

select
    service_request_id,

    -- ── timestamps (pass through) ─────────────────────────────────────
    created_at,
    closed_at,
    due_at,
    resolution_updated_at,

    -- ── agency ────────────────────────────────────────────────────────
    upper({{ clean_sentinel('agency_code') }})                 as agency_code,
    {{ clean_sentinel('agency_name') }}                        as agency_name,

    -- ── classification (Problem / Problem Detail / Additional Details) ─
    {{ clean_sentinel('complaint_type') }}                     as complaint_type,
    {{ clean_sentinel('descriptor') }}                         as descriptor,
    {{ clean_sentinel('descriptor_2') }}                       as descriptor_2,
    {{ clean_sentinel('resolution_description') }}             as resolution_description,

    -- ── status (canonical UPPER, default UNSPECIFIED) ─────────────────
    coalesce(upper({{ clean_sentinel('status') }}), 'UNSPECIFIED') as status,

    -- ── channel (canonical UPPER, default UNKNOWN) ────────────────────
    coalesce(upper({{ clean_sentinel('open_data_channel_type') }}), 'UNKNOWN') as channel_type,

    -- ── borough (submitter borough -> park borough -> UNSPECIFIED) ────
    coalesce(
        upper({{ clean_sentinel('borough') }}),
        upper({{ clean_sentinel('park_borough') }}),
        'UNSPECIFIED'
    )                                                          as borough,

    -- ── location text ─────────────────────────────────────────────────
    {{ clean_sentinel('location_type') }}                      as location_type,
    {{ clean_sentinel('incident_address') }}                   as incident_address,
    {{ clean_sentinel('street_name') }}                        as street_name,
    {{ clean_sentinel('city') }}                               as city,
    {{ clean_sentinel('landmark') }}                           as landmark,
    {{ clean_sentinel('facility_type') }}                      as facility_type,
    upper({{ clean_sentinel('address_type') }})                as address_type,
    {{ clean_sentinel('community_board') }}                    as community_board,
    {{ clean_sentinel('council_district') }}                   as council_district,
    {{ clean_sentinel('police_precinct') }}                    as police_precinct,
    {{ clean_sentinel('bbl') }}                                as bbl,

    -- ── ZIP: keep only valid 5-digit codes ────────────────────────────
    case
        when regexp_like({{ clean_sentinel('incident_zip') }}, '^[0-9]{5}$')
        then {{ clean_sentinel('incident_zip') }}
    end                                                        as incident_zip,

    -- ── TLC / bridge-highway sub-domains ──────────────────────────────
    {{ clean_sentinel('vehicle_type') }}                       as vehicle_type,
    {{ clean_sentinel('taxi_company_borough') }}               as taxi_company_borough,
    {{ clean_sentinel('taxi_pick_up_location') }}              as taxi_pick_up_location,
    upper({{ clean_sentinel('road_ramp') }})                   as road_ramp,

    -- ── geo: keep coordinates only inside the NYC bounding box ────────
    case
        when latitude  between {{ var('nyc_lat_min') }} and {{ var('nyc_lat_max') }}
         and longitude between {{ var('nyc_lon_min') }} and {{ var('nyc_lon_max') }}
        then true else false
    end                                                        as has_valid_geo,
    case
        when latitude  between {{ var('nyc_lat_min') }} and {{ var('nyc_lat_max') }}
         and longitude between {{ var('nyc_lon_min') }} and {{ var('nyc_lon_max') }}
        then latitude
    end                                                        as latitude,
    case
        when latitude  between {{ var('nyc_lat_min') }} and {{ var('nyc_lat_max') }}
         and longitude between {{ var('nyc_lon_min') }} and {{ var('nyc_lon_max') }}
        then longitude
    end                                                        as longitude,
    x_coordinate_state_plane,
    y_coordinate_state_plane,

    -- ── audit ─────────────────────────────────────────────────────────
    ingested_at

from bronze
