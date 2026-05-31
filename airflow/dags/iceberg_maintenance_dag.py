from datetime import datetime, timedelta
import logging
import runpy

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

MAINTENANCE_SCRIPT_PATH = "/opt/airflow/flink_jobs/iceberg_maintenance.py"

default_args = {
    "owner":            "crypto-streaming",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

def run_maintenance(**context):
    """
    Executes the PyIceberg maintenance script inside an isolated runtime context.
    """
    logger.info(f"Triggering Iceberg maintenance...")
    try:
        runpy.run_path(MAINTENANCE_SCRIPT_PATH, run_name="__main__")
        logger.info("Iceberg maintenance script executed successfully.")
    except Exception as e:
        logger.error(f"Maintenance script execution failed: {e}")
        raise


with DAG(
    dag_id="iceberg_maintenance_dag",
    description="Daily Iceberg maintenance: lightweight snapshot expiration via PyIceberg",
    schedule_interval="0 3 * * *",  # Runs daily at 03:00 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["iceberg", "maintenance", "pyiceberg"],
) as dag:

    run_iceberg_maintenance = PythonOperator(
        task_id="run_iceberg_maintenance",
        python_callable=run_maintenance,
    )