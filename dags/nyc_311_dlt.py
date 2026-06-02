import json
import os
import re
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable

import dlt
import pendulum
import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context


# Airflow Dataset representing the raw landed table. dlt load tasks "produce" it;
# the dbt transform DAG (dags/nyc_311_dbt_local.py) is scheduled on it, so a
# successful load auto-triggers the transform. Datasets are matched by URI, so
# the dbt DAG declares an identical Dataset(...) with this same string.
NYC_311_SERVICE_REQUESTS = Dataset("snowflake://NYC311/NYC_311/SERVICE_REQUESTS_DLT")


# This file keeps Airflow responsible for orchestration and lets dlt handle the
# destination-specific loading details. That gives us one Socrata extractor and
# lets us target Snowflake (and other destinations) via environment variables.
#
# There are two DAGs:
# 1. nyc_311_dlt_incremental: scheduled daily, reloads a recent overlap window.
# 2. nyc_311_dlt_backfill_monthly: manual, creates one task per historical month.
#
# Both DAGs still use Socrata pagination. The difference from the first custom
# ClickHouse implementation is that dlt now performs the warehouse load and
# merge/upsert behavior for each configured destination.

DATASET_FIELDS = [
    "unique_key",
    "created_date",
    "closed_date",
    "agency",
    "agency_name",
    "complaint_type",
    "descriptor",
    "descriptor_2",
    "location_type",
    "incident_zip",
    "incident_address",
    "street_name",
    "cross_street_1",
    "cross_street_2",
    "intersection_street_1",
    "intersection_street_2",
    "address_type",
    "city",
    "landmark",
    "facility_type",
    "status",
    "due_date",
    "resolution_description",
    "resolution_action_updated_date",
    "community_board",
    "council_district",
    "police_precinct",
    "bbl",
    "borough",
    "x_coordinate_state_plane",
    "y_coordinate_state_plane",
    "open_data_channel_type",
    "park_facility_name",
    "park_borough",
    "vehicle_type",
    "taxi_company_borough",
    "taxi_pick_up_location",
    "bridge_highway_name",
    "bridge_highway_direction",
    "road_ramp",
    "bridge_highway_segment",
    "latitude",
    "longitude",
    "location",
]

DATETIME_FIELDS = {
    "created_date",
    "closed_date",
    "due_date",
    "resolution_action_updated_date",
}

DOUBLE_FIELDS = {
    "x_coordinate_state_plane",
    "y_coordinate_state_plane",
    "latitude",
    "longitude",
}

BASE_DIR = Path("/opt/airflow")
RAW_DIR = BASE_DIR / "data" / "raw"
STAGING_DIR = BASE_DIR / "data" / "staging"


def env_int(name: str, default: int) -> int:
    """Read an integer environment variable with a fallback default."""
    raw = os.getenv(name)
    return int(raw) if raw else default


def configured_destinations(conf: dict[str, Any] | None = None) -> list[str]:
    """Return the destination names this run should load.

    Airflow trigger config can override the environment, for example:
    {"destinations": ["clickhouse", "snowflake"]}.
    """
    conf = conf or {}
    value = conf.get("destinations", os.getenv("NYC_311_DESTINATIONS", "snowflake"))
    if isinstance(value, str):
        destinations = [item.strip() for item in value.split(",")]
    else:
        destinations = [str(item).strip() for item in value]
    return [item for item in destinations if item]


def parse_datetime(value: Any):
    """Convert Socrata timestamps into Python datetime objects for dlt."""
    if value in (None, ""):
        return None
    try:
        return pendulum.parse(str(value)).naive()
    except Exception:
        return None


def parse_float(value: Any) -> float | None:
    """Convert numeric strings into floats while keeping bad/missing values null."""
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_record(
    record: dict[str, Any],
    ingested_at,
    dataset_id: str,
    window: dict[str, Any],
) -> dict[str, Any] | None:
    """Convert one Socrata API row into a destination-neutral dlt row.

    unique_key is the business primary key. dlt uses it for merge/upsert loads
    in destinations that support merge, including Snowflake and ClickHouse.
    """
    unique_key = record.get("unique_key")
    if unique_key in (None, ""):
        return None

    normalized: dict[str, Any] = {}
    for field in DATASET_FIELDS:
        value = record.get(field)
        if field == "unique_key":
            try:
                normalized[field] = int(value)
            except (TypeError, ValueError):
                return None
        elif field in DATETIME_FIELDS:
            normalized[field] = parse_datetime(value)
        elif field in DOUBLE_FIELDS:
            normalized[field] = parse_float(value)
        elif field == "location" and value not in (None, ""):
            normalized[field] = json.dumps(value, separators=(",", ":"), sort_keys=True)
        else:
            normalized[field] = value

    normalized["ingested_at"] = ingested_at
    normalized["source_dataset"] = dataset_id
    normalized["source_window_start"] = parse_datetime(window["start"])
    normalized["source_window_end"] = parse_datetime(window["end"])
    return normalized


def http_request_with_retries(method: str, url: str, **kwargs):
    """Run an HTTP request with retry handling for transient API failures."""
    retry_statuses = {429, 500, 502, 503, 504}
    last_error = None
    for attempt in range(1, 6):
        try:
            response = requests.request(method, url, timeout=120, **kwargs)
            if response.status_code not in retry_statuses:
                return response
            last_error = RuntimeError(f"{response.status_code}: {response.text[:500]}")
        except requests.RequestException as exc:
            last_error = exc
        time.sleep(min(60, 2**attempt))
    raise RuntimeError(f"HTTP request failed after retries: {last_error}")


def socrata_pages(window: dict[str, Any]) -> Iterable[list[dict[str, Any]]]:
    """Yield raw Socrata pages for a bounded created_date window.

    This is where pagination lives. A month with 200,000 rows becomes four API
    calls when page_limit is 50,000.
    """
    dataset_id = os.getenv("NYC_311_DATASET_ID", "erm2-nwe9")
    domain = os.getenv("NYC_311_API_DOMAIN", "data.cityofnewyork.us")
    app_token = os.getenv("SOCRATA_APP_TOKEN")
    api_url = f"https://{domain}/resource/{dataset_id}.json"
    headers = {"X-App-Token": app_token} if app_token else {}

    page_limit = int(window["page_limit"])
    max_pages = window.get("max_pages")
    offset = 0
    page_count = 0
    where_clause = f"created_date >= '{window['start']}' AND created_date < '{window['end']}'"

    while True:
        if max_pages is not None and page_count >= max_pages:
            break

        response = http_request_with_retries(
            "GET",
            api_url,
            headers=headers,
            params={
                "$limit": page_limit,
                "$offset": offset,
                "$order": "created_date, unique_key",
                "$where": where_clause,
            },
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Socrata request failed: HTTP {response.status_code} {response.text[:1000]}")

        page = response.json()
        if not isinstance(page, list):
            raise RuntimeError(f"Unexpected Socrata response: {str(page)[:1000]}")
        if not page:
            break

        page_name = f"{window['start'].replace(':', '')}_{offset:012d}"
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        (RAW_DIR / f"nyc311_{page_name}.json").write_text(json.dumps(page, separators=(",", ":")))

        yield page

        if len(page) < page_limit:
            break
        page_count += 1
        offset += page_limit


def service_request_rows(window: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Yield normalized rows from all pages in one Airflow window."""
    dataset_id = os.getenv("NYC_311_DATASET_ID", "erm2-nwe9")
    ingested_at = pendulum.now("UTC").naive()
    row_count = 0

    for page in socrata_pages(window):
        normalized_page = []
        for record in page:
            normalized = normalize_record(record, ingested_at, dataset_id, window)
            if normalized is not None:
                normalized_page.append(normalized)

        if normalized_page:
            STAGING_DIR.mkdir(parents=True, exist_ok=True)
            staging_path = STAGING_DIR / f"nyc311_dlt_{window['start'].replace(':', '')}_{row_count:012d}.json"
            staging_path.write_text(json.dumps(normalized_page, default=str, separators=(",", ":")))

        for normalized in normalized_page:
            row_count += 1
            yield normalized


def destination_dataset_name(destination: str) -> str | None:
    """Choose the dlt dataset/schema name for each destination.

    ClickHouse has no real schemas, but dlt still needs a dataset concept. dlt
    implements that by prefixing table names inside the configured database.
    Snowflake uses dataset_name as a normal schema name.
    """
    if destination == "clickhouse":
        return os.getenv("NYC_311_DLT_CLICKHOUSE_DATASET", "nyc_311_dlt")
    return os.getenv("NYC_311_DLT_SNOWFLAKE_DATASET", "NYC_311")


def make_resource(window: dict[str, Any]):
    """Create a dlt resource for one window.

    write_disposition="merge" tells dlt that repeated loads of the same
    unique_key should update/deduplicate instead of appending duplicates.
    """
    table_name = os.getenv("NYC_311_DLT_TABLE_NAME", "service_requests_dlt")

    @dlt.resource(
        name=table_name,
        primary_key="unique_key",
        write_disposition="merge",
    )
    def service_requests():
        yield from service_request_rows(window)

    return service_requests()


def run_dlt_for_window(window: dict[str, Any], destinations: list[str]) -> list[dict[str, Any]]:
    """Load one Airflow window into every configured dlt destination."""
    results = []
    window_id = window["start"][:7].replace("-", "_")
    context = get_current_context()
    run_id = re.sub(r"[^A-Za-z0-9_]", "_", context["run_id"])
    try_number = context["ti"].try_number

    for destination in destinations:
        pipeline = dlt.pipeline(
            pipeline_name=f"nyc_311_dlt_{destination}_{window_id}_{run_id}_try_{try_number}",
            destination=destination,
            dataset_name=destination_dataset_name(destination),
        )
        load_info = pipeline.run(make_resource(window))
        results.append(
            {
                "destination": destination,
                "window_start": window["start"],
                "window_end": window["end"],
                "load_info": str(load_info),
            }
        )
    return results


def context_conf() -> dict[str, Any]:
    """Read Airflow trigger configuration in a small reusable helper."""
    dag_run = get_current_context().get("dag_run")
    return (dag_run.conf or {}) if dag_run else {}


def build_window(start, end, conf: dict[str, Any]) -> dict[str, Any]:
    """Return the normalized window dict passed between Airflow tasks."""
    return {
        "start": start.in_timezone("UTC").format("YYYY-MM-DDTHH:mm:ss"),
        "end": end.in_timezone("UTC").format("YYYY-MM-DDTHH:mm:ss"),
        "page_limit": int(conf.get("page_limit", env_int("NYC_311_PAGE_LIMIT", 50000))),
        "max_pages": int(conf["max_pages"]) if conf.get("max_pages") not in (None, "") else None,
    }


@dag(
    dag_id="nyc_311_dlt_incremental",
    description="Daily dlt incremental load of NYC 311 service requests into configured destinations.",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["nyc", "socrata", "dlt", "incremental"],
)
def nyc_311_dlt_incremental():
    @task
    def resolve_window() -> dict[str, Any]:
        conf = context_conf()
        if conf.get("start_date"):
            start = pendulum.parse(conf["start_date"])
            end = pendulum.parse(conf["end_date"]) if conf.get("end_date") else start.add(days=1)
        else:
            interval_end = get_current_context()["data_interval_end"]
            end = pendulum.instance(interval_end).in_timezone("UTC")
            overlap_days = int(conf.get("overlap_days", env_int("NYC_311_OVERLAP_DAYS", 3)))
            start = end.subtract(days=overlap_days)
        return build_window(start, end, conf)

    # Producing this Dataset on success auto-triggers nyc_311_dbt_local.
    @task(outlets=[NYC_311_SERVICE_REQUESTS])
    def load_window(window: dict[str, Any]) -> list[dict[str, Any]]:
        return run_dlt_for_window(window, configured_destinations(context_conf()))

    load_window(resolve_window())


@dag(
    dag_id="nyc_311_dlt_backfill_monthly",
    description="Manual month-by-month dlt historical backfill of NYC 311 service requests.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["nyc", "socrata", "dlt", "backfill"],
)
def nyc_311_dlt_backfill_monthly():
    @task
    def build_month_windows() -> list[dict[str, Any]]:
        conf = context_conf()
        start_month = conf.get("start_month", "2020-01")
        end_month = conf.get("end_month", pendulum.now("UTC").format("YYYY-MM"))

        cursor = pendulum.parse(f"{start_month}-01T00:00:00Z")
        end = pendulum.parse(f"{end_month}-01T00:00:00Z")
        windows = []
        while cursor < end:
            windows.append(build_window(cursor, cursor.add(months=1), conf))
            cursor = cursor.add(months=1)
        return windows

    @task
    def load_month(window: dict[str, Any]) -> list[dict[str, Any]]:
        return run_dlt_for_window(window, configured_destinations(context_conf()))

    loaded = load_month.expand(window=build_month_windows())

    # Emit the Dataset once, after ALL months load, so the dbt transform runs a
    # single time at the end of a backfill (not once per month).
    signal_dataset_updated = EmptyOperator(
        task_id="signal_dataset_updated",
        outlets=[NYC_311_SERVICE_REQUESTS],
    )
    loaded >> signal_dataset_updated


nyc_311_dlt_incremental()
nyc_311_dlt_backfill_monthly()
