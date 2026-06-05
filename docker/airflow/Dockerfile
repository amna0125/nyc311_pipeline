FROM apache/airflow:3.2.2

# dlt handles extraction/loading; dbt runs in dbt Cloud (not in this image).
# The dbt-cloud provider lets Airflow trigger a dbt Cloud job via its API.
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "dlt[snowflake]==1.27.2" \
    "apache-airflow-providers-dbt-cloud==4.*"
