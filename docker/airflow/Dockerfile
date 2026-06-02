FROM apache/airflow:2.10.5

# git is used by dbt for some operations and satisfies `dbt debug`'s dependency check.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir \
    "dlt[snowflake]==1.27.2" \
    requests==2.32.3

# dbt runs locally inside this image. Install it into an isolated virtualenv so
# its dependency tree cannot clash with Airflow's pinned packages. The DAG calls
# the binary at /home/airflow/dbt-venv/bin/dbt.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-snowflake==1.8.*"
