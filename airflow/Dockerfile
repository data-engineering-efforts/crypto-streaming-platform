FROM apache/airflow:2.9.0-python3.11

USER airflow
RUN pip install --no-cache-dir \
    clickhouse-driver \
    docker \
    "pyiceberg[s3fs,nessie]"