import subprocess
import logging
import os

logger = logging.getLogger(__name__)

FLINK_JOB_MANAGER = "flink-jobmanager"
FLINK_JOBS_DIR    = "/opt/flink/usrlib"


def _copy_and_start(local_path: str) -> subprocess.Popen:
    """
    Copy Python file to Flink JobManager and start process.
    Helper for run_flink_file and run_flink_query.
    """
    container_path = f"/tmp/{os.path.basename(local_path)}"

    subprocess.run(
        ["docker", "cp", local_path,
         f"{FLINK_JOB_MANAGER}:{container_path}"],
        capture_output=True,
        text=True,
        check=True,
    )

    return subprocess.Popen(
        ["docker", "exec",
         "-e", f"PYTHONPATH={FLINK_JOBS_DIR}",
         FLINK_JOB_MANAGER,
         "python3", container_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def run_flink_file(local_path: str, timeout: int = 120, log_prefix: str = "") -> None:
    """
    Copy and run Python file in Flink JobManager.
    Streams output in real time via logger.
    Use for jobs that don't return values (reconciliation, maintenance).
    """
    process = _copy_and_start(local_path)

    for line in process.stdout:
        line = line.rstrip()
        if line:
            logger.info(f"[{log_prefix}] {line}" if log_prefix else line)

    process.wait()

    if process.returncode != 0:
        raise RuntimeError(f"Job failed with exit code {process.returncode}")


def run_flink_query(local_path: str, timeout: int = 120) -> str:
    """
    Copy and run Python file in Flink JobManager.
    Returns stdout as string.
    Use for utility scripts that return a value (check_iceberg_data).
    """
    process = _copy_and_start(local_path)
    stdout, _ = process.communicate(timeout=timeout)

    if process.returncode != 0:
        raise RuntimeError(
            f"Query failed with exit code {process.returncode}\n"
            f"Output: {stdout}"
        )

    return stdout.strip()