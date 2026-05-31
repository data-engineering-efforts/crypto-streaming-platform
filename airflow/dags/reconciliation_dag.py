from datetime import datetime, timedelta, timezone
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

from operators.flink_operator import run_flink_file, run_flink_query
from operators.clickhouse_operator import get_recon_results

logger = logging.getLogger(__name__)

CHECK_ICEBERG_JOB  = "/opt/airflow/flink_jobs/check_iceberg_data.py"
RECONCILIATION_JOB = "/opt/airflow/flink_jobs/reconciliation_job.py"

default_args = {
    "owner": "crypto-streaming",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def check_iceberg_data(**context):
    stdout = run_flink_query(CHECK_ICEBERG_JOB)

    # Assuming the check_iceberg_data.py script prints only the count as the last line.
    last_line = stdout.strip().split("\n")[-1].strip()
    count = int(last_line)

    logger.info(f"Iceberg records for last hour: {count}")

    if count == 0:
        raise AirflowSkipException("No data in Iceberg")

    return count

def run_reconciliation(**context):
    """Run reconciliation batch job in Flink."""
    run_flink_file(
        RECONCILIATION_JOB,
        log_prefix="Reconciliation",
        timeout=600,
    )

def check_alerts(**context):
    """Check recon_results for ALERTs."""
    now = datetime.now(timezone.utc)
    window_start = now.replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(hours=1)

    rows = get_recon_results(window_start)
    alerts = [r for r in rows if r[4] == "ALERT"]
    oks = [r for r in rows if r[4] == "OK"]

    logger.info(f"Results: {len(oks)} OK, {len(alerts)} ALERTS")

    for r in oks:
        logger.info(f"OK: {r[0]} diff={r[3]:.4f}%")

    for r in alerts:
        logger.warning(
            f"ALERT: {r[0]} "
            f"streaming={r[1]:.4f} "
            f"batch={r[2]:.4f} "
            f"diff={r[3]:.4f}%"
        )

    if alerts:
        logger.warning(
            f"Found {len(alerts)} ALERT(s), "
            f"check recon_results table for details."
        )

with DAG(
    dag_id="reconciliation_dag",
    description="Hourly VWAP reconciliation: Iceberg (batch) vs ClickHouse (streaming)",
    schedule_interval="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["reconciliation", "iceberg", "clickhouse"],
) as dag:

    check_data = PythonOperator(task_id="check_iceberg_data", python_callable=check_iceberg_data)
    run_recon = PythonOperator(task_id="run_reconciliation", python_callable=run_reconciliation)
    check_results = PythonOperator(task_id="check_alerts", python_callable=check_alerts)

    check_data >> run_recon >> check_results