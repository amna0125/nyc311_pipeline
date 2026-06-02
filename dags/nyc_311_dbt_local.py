from datetime import timedelta

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator


# Same Dataset URI the dlt DAGs produce (Airflow matches datasets by URI). The
# DAG below is scheduled on it, so a successful dlt load auto-triggers dbt.
NYC_311_SERVICE_REQUESTS = Dataset("snowflake://NYC311/NYC_311/SERVICE_REQUESTS_DLT")


# This DAG runs dbt locally inside the Airflow image (no dbt Cloud). It builds
# the medallion models (bronze -> silver -> gold) and runs the data-quality +
# schema-drift tests against the data the dlt DAGs land in Snowflake
# (NYC_311.SERVICE_REQUESTS_DLT).
#
# dbt is installed in an isolated virtualenv (see docker/airflow/Dockerfile) so
# its dependencies don't clash with Airflow. We call that binary directly.
#
# Setup: the Snowflake credentials come from the existing DESTINATION__SNOWFLAKE__*
# vars in .env (resolved by dbt/profiles.yml). DBT_PROFILES_DIR / DBT_PROJECT_DIR
# are set in docker-compose.yml.

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt"
# Keep dbt's compiled output off the host-mounted project volume to avoid
# container-write/permission issues.
DBT_TARGET_PATH = "/tmp/dbt_target"


@dag(
    dag_id="nyc_311_dbt_local",
    description="Run dbt (local, in-container) to build the NYC 311 medallion models and tests.",
    # Data-aware scheduling: runs when the dlt load produces NYC_311_SERVICE_REQUESTS,
    # so the transform fires right after a successful load (not on a fixed timer).
    schedule=[NYC_311_SERVICE_REQUESTS],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["nyc", "dbt", "transform"],
)
def nyc_311_dbt_local():
    # Install dbt package dependencies (dbt_utils, codegen, dbt_expectations).
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} deps",
    )

    # `dbt build` runs seeds + models + tests in DAG order
    # (bronze -> silver -> gold + integrity/drift tests).
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BIN} build --target prod --target-path {DBT_TARGET_PATH}"
        ),
    )

    dbt_deps >> dbt_build


nyc_311_dbt_local()
