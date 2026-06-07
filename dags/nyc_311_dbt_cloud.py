import os

import pendulum
from airflow.decorators import dag
from airflow.sdk import Asset
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator


# Transformation half of the pipeline, running in dbt Cloud.
#
# Airflow only ORCHESTRATES: when the dlt load produces the dataset below, this
# DAG triggers a dbt Cloud job (via the dbt Cloud API) that builds the medallion
# models + tests. dbt itself runs in dbt Cloud against the project in its own
# GitHub repo — see the standalone dbt repo's README for the Cloud setup.
#
# Same Asset URI the dlt DAGs produce (Airflow matches assets by URI), so a
# successful load auto-triggers this - same event trigger the old local dbt DAG used.
def snowflake_asset_uri() -> str:
    account = os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__HOST") or "local-dev"
    database = os.getenv("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE") or "NYC311"
    schema = os.getenv("NYC_311_DLT_SNOWFLAKE_DATASET") or "NYC_311"
    table = (os.getenv("NYC_311_DLT_TABLE_NAME") or "service_requests_dlt").upper()
    return f"snowflake://{account}/{database}/{schema}/{table}"


NYC_311_SERVICE_REQUESTS = Asset(snowflake_asset_uri())


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw else default


@dag(
    dag_id="nyc_311_dbt_cloud",
    description="Trigger the dbt Cloud job that builds the NYC 311 medallion models and tests.",
    # Data-aware scheduling: fires when the dlt load produces NYC_311_SERVICE_REQUESTS.
    schedule=[NYC_311_SERVICE_REQUESTS],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["nyc", "dbt", "dbt-cloud", "transform"],
)
def nyc_311_dbt_cloud():
    DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job",
        dbt_cloud_conn_id="dbt_cloud_default",
        job_id=env_int("DBT_CLOUD_JOB_ID", 0),
        # Block until the dbt Cloud run finishes so the Airflow task reflects its
        # real status (and anything downstream can depend on a successful build).
        wait_for_termination=True,
        check_interval=30,
        timeout=3600,
    )


nyc_311_dbt_cloud()
