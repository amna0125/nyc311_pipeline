{{ config(severity='warn', tags=['drift'], store_failures=true) }}

/*
    Schema-drift guard for the NYC 311 source table.

    Compares the LIVE column set of NYC_311.SERVICE_REQUESTS_DLT (from Snowflake's
    INFORMATION_SCHEMA) against the maintained manifest seed_expected_source_columns.
    Returns rows — and therefore fails — when the schemas diverge:

      MISSING/RENAMED : an expected column is no longer present (dropped/renamed
                        upstream). This usually means downstream models will break
                        or silently lose data.
      NEW/UNMAPPED    : a column exists in the warehouse that we never mapped
                        (the API/dlt added a field). Bronze selects columns
                        explicitly, so a new column is silently ignored until it
                        is reviewed and mapped here.

    Severity is `warn` so a new column doesn't halt production loads; flip to
    `error` (or split the MISSING branch to error) if you want renames/removals
    to hard-fail the pipeline. Failing rows are stored for triage.

    Runbook on a hit: review the upstream change, then update
    seed_expected_source_columns.csv, the bronze model, and the data dictionary.
*/

{%- set src = source('nyc_311', 'service_requests_dlt') -%}

with live as (
    select lower(column_name) as column_name
    from {{ src.database }}.information_schema.columns
    where upper(table_schema) = upper('{{ src.schema }}')
      and upper(table_name)   = upper('{{ src.identifier }}')
),

expected as (
    select lower(column_name) as column_name
    from {{ ref('seed_expected_source_columns') }}
),

missing as (
    select 'MISSING/RENAMED' as issue, column_name from expected
    except
    select 'MISSING/RENAMED' as issue, column_name from live
),

new_columns as (
    select 'NEW/UNMAPPED' as issue, column_name from live
    except
    select 'NEW/UNMAPPED' as issue, column_name from expected
)

select * from missing
union all
select * from new_columns
