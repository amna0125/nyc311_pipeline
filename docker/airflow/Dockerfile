FROM apache/airflow:2.10.5

# dlt handles extraction/loading; dbt runs in dbt Cloud (not in this image).
# The dbt-cloud provider lets Airflow trigger a dbt Cloud job via its API.
RUN pip install --no-cache-dir \
    "dlt[snowflake]==1.27.2" \
    requests==2.32.3 \
    "apache-airflow-providers-dbt-cloud==3.*"
