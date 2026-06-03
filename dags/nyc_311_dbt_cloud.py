import os

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator


# Transformation half of the pipeline, running in dbt Cloud (not locally).
#
# Airflow only ORCHESTRATES: when the dlt load produces the dataset below, this
# DAG triggers a dbt Cloud job (via the dbt Cloud API) that builds the medallion
# models + tests. dbt itself runs in dbt Cloud against the project in its own
# GitHub repo — see the standalone dbt repo's README for the Cloud setup.
#
# Same Dataset URI the dlt DAGs produce (Airflow matches datasets by URI), so a
# successful load auto-triggers this — same event trigger the old local dbt DAG used.
NYC_311_SERVICE_REQUESTS = Dataset("snowflake://NYC311/NYC_311/SERVICE_REQUESTS_DLT")


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
