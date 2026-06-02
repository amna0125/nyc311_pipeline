# NYC 311 dbt Project (Medallion: bronze → silver → gold)

Transforms the raw NYC 311 service requests landed in Snowflake by the dlt
pipeline (`../dags/nyc_311_dlt.py`) into clean, conformed, analytics-ready models
following the Medallion architecture.

```
source:  NYC311.NYC_311.SERVICE_REQUESTS_DLT   (raw, loaded by dlt)
  └─ bronze  stg_nyc311__service_requests       (1:1 mirror, typed/renamed, views)
       └─ silver  int_service_requests__cleaned   (scrub sentinels, standardize)
              int_service_requests__enriched      (derived measures + flags)
                └─ gold   dim_date + fct_service_requests (wide) + mart_*
```

## Layers

| Layer | Schema | Models | Materialization |
|---|---|---|---|
| Bronze | `NYC_311_BRONZE` | `stg_nyc311__service_requests` | view |
| Silver | `NYC_311_SILVER` | `int_service_requests__cleaned`, `int_service_requests__enriched` | view |
| Gold | `NYC_311_GOLD` | `dim_date`, `fct_service_requests`, `mart_*` | table / incremental |

### Gold modeling choice — wide fact + one conformed dimension

Rather than a full star schema, gold uses a **wide fact** (`fct_service_requests`):
descriptive attributes (borough, agency, complaint type, status, channel,
location) live on the fact as **degenerate dimensions**, because they are plain
labels with no extra attributes — a separate dim table would add only a join.
The single dimension kept as its own table is **`dim_date`** (a gap-free calendar
with weekend/month/quarter attributes you can't derive from the fact alone). The
marts read directly from the wide fact, so there is no normalize-then-re-join
round-trip. This suits a single-fact dataset on columnar Snowflake.

`fct_service_requests` is **incremental** (`merge` on `service_request_id`,
filtered on `ingested_at`) so daily dlt overlap reloads update mutated rows
(status flips, late `closed_date`) in place instead of duplicating.

## Data quality & schema-drift tests

- **Integrity** (`tag:integrity`, in `models/silver/_silver__models.yml`): value
  validation from the data dictionary. Truly closed sets (borough, channel) are
  `error`; evolving/non-exhaustive sets (agency, location_type, status,
  facility_type, and — proven by real data — address_type, road_ramp) are `warn`.
  Structural checks: ZIP `^[0-9]{5}$`, BBL `^[1-5][0-9]{9}$`, lat/long NYC
  bounding box, positive State Plane coords, non-negative resolution time, no
  future dates. `closed_at >= created_at` is `warn` (a known NYC 311 artifact the
  enriched model already neutralizes).
- **Schema drift** (`tag:drift`, `tests/assert_source_schema_unchanged.sql`):
  diffs the live `INFORMATION_SCHEMA` columns of the source against
  `seeds/seed_expected_source_columns.csv` and flags `MISSING/RENAMED` and
  `NEW/UNMAPPED` columns. Bronze also selects columns explicitly, so a dropped/
  renamed column breaks the build loudly.
- Failing test rows are stored (`+store_failures: true`) in schema
  `NYC_311_DQ_FAILURES` for triage.

When drift is flagged: review the API change, then update
`seed_expected_source_columns.csv`, the bronze model, and the data dictionary.

## Run via Airflow (primary)

dbt runs **locally inside the Airflow image** — no dbt Cloud. The
`docker/airflow/Dockerfile` installs `dbt-snowflake` into an isolated venv
(`/home/airflow/dbt-venv`), `docker-compose.yml` mounts this directory to
`/opt/airflow/dbt` and sets `DBT_PROFILES_DIR` / `DBT_PROJECT_DIR`, and
[`profiles.yml`](profiles.yml) reuses the `DESTINATION__SNOWFLAKE__CREDENTIALS__*`
vars from `../.env`.

1. Build/refresh the image after the Dockerfile change:
   ```bash
   docker compose build && docker compose up -d
   ```
2. In the Airflow UI, run the **`nyc_311_dbt_local`** DAG. It runs:
   - `dbt_deps`  → installs dbt_utils, codegen, dbt_expectations
   - `dbt_build` → seeds + models + tests in DAG order (bronze → silver → gold)

   Chain it after `nyc_311_dlt_incremental` (or trigger manually) so it runs on
   freshly loaded data.

## Run from the CLI (development / debugging)

Inside the running scheduler container (creds already injected from `.env`):

```bash
docker compose exec airflow-scheduler bash
cd /opt/airflow/dbt
DBT=/home/airflow/dbt-venv/bin/dbt

$DBT deps                         # install packages
$DBT debug                        # verify the Snowflake connection
$DBT build --target-path /tmp/dbt_target            # all models + tests
# useful subsets:
$DBT test --select tag:integrity
$DBT test --select assert_source_schema_unchanged
$DBT build --select fct_service_requests            # after a dlt load (incremental)
$DBT build --full-refresh --select fct_service_requests   # once after backfill
```

To run on your host machine instead, `pip install dbt-snowflake`, export the
`DESTINATION__SNOWFLAKE__CREDENTIALS__*` vars, set
`DBT_PROFILES_DIR=$(pwd)` from this directory, and use the same commands.

## Layout

```
dbt_project.yml          layer materializations, schemas, NYC geo vars
profiles.yml             Snowflake connection (reuses .env DESTINATION__SNOWFLAKE vars)
packages.yml             dbt_utils, codegen, dbt_expectations
macros/clean_sentinel.sql
models/bronze/           sources.yml + stg model
models/silver/           cleaned + enriched + integrity tests
models/gold/{dimensions,facts,marts}/
seeds/                   agency codes, location types, expected source columns
tests/assert_source_schema_unchanged.sql
```
