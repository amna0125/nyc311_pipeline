{{ config(materialized='view') }}

/*
    Bronze: faithful 1:1 mirror of the dlt landing table.
    - Rename to clean, consistent names.
    - Cast to proper types (datetimes -> timestamp_ntz, coords -> float).
    - Parse the `location` JSON string into a VARIANT.
    - Carry dlt + pipeline audit columns for lineage.
    - Defensive dedup: dlt merge already guarantees one row per unique_key, but
      this protects against a partial/failed merge load leaving duplicates.

    NOTE: columns are selected EXPLICITLY (no select *). This is intentional — if
    the upstream API/dlt renames or drops a column, this model fails to build
    loudly instead of silently producing wrong data (schema-drift guard #1).
*/

with source as (
    select * from {{ source('nyc_311', 'service_requests_dlt') }}
),

renamed as (
    select
        -- ── business key ──────────────────────────────────────────────
        cast(unique_key as integer)                            as service_request_id,

        -- ── timestamps (naive datetimes from dlt) ─────────────────────
        cast(created_date as timestamp_ntz)                    as created_at,
        cast(closed_date as timestamp_ntz)                     as closed_at,
        cast(due_date as timestamp_ntz)                        as due_at,
        cast(resolution_action_updated_date as timestamp_ntz)  as resolution_updated_at,

        -- ── agency / classification ───────────────────────────────────
        trim(agency)                                           as agency_code,
        trim(agency_name)                                      as agency_name,
        trim(complaint_type)                                   as complaint_type,   -- "Problem"
        trim(descriptor)                                       as descriptor,        -- "Problem Detail"
        trim(descriptor_2)                                     as descriptor_2,      -- "Additional Details"
        trim(status)                                           as status,
        trim(resolution_description)                           as resolution_description,

        -- ── location text ─────────────────────────────────────────────
        trim(location_type)                                    as location_type,
        trim(incident_zip)                                     as incident_zip,
        trim(incident_address)                                 as incident_address,
        trim(street_name)                                      as street_name,
        trim(cross_street_1)                                   as cross_street_1,
        trim(cross_street_2)                                   as cross_street_2,
        trim(intersection_street_1)                            as intersection_street_1,
        trim(intersection_street_2)                            as intersection_street_2,
        trim(address_type)                                     as address_type,
        trim(city)                                             as city,
        trim(landmark)                                         as landmark,
        trim(facility_type)                                    as facility_type,
        trim(borough)                                          as borough,
        trim(community_board)                                  as community_board,
        trim(council_district)                                 as council_district,
        trim(police_precinct)                                  as police_precinct,
        trim(bbl)                                              as bbl,
        trim(open_data_channel_type)                           as open_data_channel_type,
        trim(park_facility_name)                               as park_facility_name,
        trim(park_borough)                                     as park_borough,

        -- ── TLC / bridge-highway sub-domains ──────────────────────────
        trim(vehicle_type)                                     as vehicle_type,
        trim(taxi_company_borough)                             as taxi_company_borough,
        trim(taxi_pick_up_location)                            as taxi_pick_up_location,
        trim(bridge_highway_name)                              as bridge_highway_name,
        trim(bridge_highway_direction)                         as bridge_highway_direction,
        trim(road_ramp)                                        as road_ramp,
        trim(bridge_highway_segment)                           as bridge_highway_segment,

        -- ── geo numerics ──────────────────────────────────────────────
        cast(x_coordinate_state_plane as float)               as x_coordinate_state_plane,
        cast(y_coordinate_state_plane as float)               as y_coordinate_state_plane,
        cast(latitude as float)                               as latitude,
        cast(longitude as float)                              as longitude,
        try_parse_json(location)                              as location_json,

        -- ── audit / lineage (pipeline + dlt) ──────────────────────────
        cast(ingested_at as timestamp_ntz)                    as ingested_at,
        source_dataset,
        cast(source_window_start as timestamp_ntz)            as source_window_start,
        cast(source_window_end as timestamp_ntz)              as source_window_end,
        _dlt_load_id,
        _dlt_id

    from source
),

deduped as (
    select *
    from renamed
    qualify row_number() over (
        partition by service_request_id
        order by ingested_at desc nulls last
    ) = 1
)

select * from deduped
