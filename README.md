# NYC 311 Airflow + dlt Pipeline

This project runs a local Apache Airflow pipeline that loads NYC Open Data 311 service requests from Socrata through dlt into one or more destinations.

Default local destination:

- ClickHouse

Optional cloud destination:

- Snowflake

Dataset:

- Socrata domain: `data.cityofnewyork.us`
- Dataset id: `erm2-nwe9`
- SODA 2.x endpoint: `https://data.cityofnewyork.us/resource/erm2-nwe9.json`

Airflow controls the time windows. dlt handles destination-specific loading, schemas, and merge/upsert behavior with `unique_key` as the primary key.

## Prerequisites

- Docker Desktop or Docker Engine with Docker Compose
- Optional Socrata app token for higher API limits
- Optional Snowflake account if loading to Snowflake

## Setup

```bash
cp .env.example .env
```

Optionally edit `.env` and set:

```bash
SOCRATA_APP_TOKEN=your_token_here
```

Start the local stack:

```bash
docker compose up --build
```

Airflow UI:

- URL: `http://localhost:8080`
- Username: `airflow`
- Password: `airflow`

ClickHouse:

- HTTP: `localhost:8123`
- Native TCP: `localhost:9000`
- User: `default`
- Password: `clickhouse`

## DAGs

There are two dlt DAGs:

```text
nyc_311_dlt_incremental
nyc_311_dlt_backfill_monthly
```

`nyc_311_dlt_incremental` runs daily and reloads a recent overlap window. The default overlap is 3 days:

```text
NYC_311_OVERLAP_DAYS=3
```

`nyc_311_dlt_backfill_monthly` is manual and creates one mapped task per month.

## Smoke Test

Trigger `nyc_311_dlt_incremental` manually with:

```json
{
  "start_date": "2026-05-31T00:00:00",
  "end_date": "2026-06-01T00:00:00",
  "page_limit": 1000,
  "max_pages": 1,
  "destinations": ["clickhouse"]
}
```

## Monthly Backfill

Trigger `nyc_311_dlt_backfill_monthly` with:

```json
{
  "start_month": "2020-01",
  "end_month": "2020-04",
  "page_limit": 50000,
  "destinations": ["clickhouse"]
}
```

`end_month` is exclusive. The example above loads:

```text
2020-01
2020-02
2020-03
```

For the full historical backfill through June 2026:

```json
{
  "start_month": "2020-01",
  "end_month": "2026-06",
  "page_limit": 50000,
  "destinations": ["clickhouse"]
}
```

The DAG still paginates inside each month with Socrata `$limit`, `$offset`, `$order`, and `$where`.

## Destinations

Default:

```text
NYC_311_DESTINATIONS=clickhouse
```

To load both ClickHouse and Snowflake:

```text
NYC_311_DESTINATIONS=clickhouse,snowflake
```

Or override per manual DAG run:

```json
{
  "start_month": "2026-05",
  "end_month": "2026-06",
  "destinations": ["clickhouse", "snowflake"]
}
```

## Snowflake Config

Set these in `.env` before using `snowflake` as a destination:

```text
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_DATABASE=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_ROLE=
```

dlt will use schema:

```text
NYC_311_DLT_SNOWFLAKE_DATASET=NYC_311
```

## Query ClickHouse

ClickHouse does not have schemas in the same way Snowflake does, so dlt prefixes table names with a virtual dataset. The dlt-managed ClickHouse table defaults to:

```text
nyc_311_dlt__service_requests_dlt
```

Query from the host:

```bash
curl -u default:clickhouse "http://localhost:8123/?query=SELECT%201"
```

Or use the native client:

```bash
docker compose exec clickhouse clickhouse-client --user default --password clickhouse
```

Example:

```sql
SELECT count()
FROM nyc_311_dlt__service_requests_dlt;

SELECT complaint_type, count() AS requests
FROM nyc_311_dlt__service_requests_dlt
GROUP BY complaint_type
ORDER BY requests DESC
LIMIT 20;
```

## Project Layout

```text
dags/nyc_311_dlt.py           Airflow DAGs and dlt source
docker-compose.yml            Local Airflow + ClickHouse stack
docker/airflow/Dockerfile     Airflow image with dlt dependencies
data/raw/                     Raw Socrata page snapshots, git-ignored
data/staging/                 Normalized JSON snapshots, git-ignored
data/dlt/                     dlt working state, git-ignored
```

## Notes

- dlt uses `unique_key` as the primary key with `write_disposition="merge"`.
- The historical backfill is month-by-month so failures can be retried at month granularity.
- The daily incremental DAG reloads a small overlap window because NYC 311 rows can change after creation.
- The pipeline skips Socrata computed region columns and loads the 44 business columns from the dataset metadata.
