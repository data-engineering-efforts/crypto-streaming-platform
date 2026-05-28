import subprocess
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

JOBS = [
    "vwap_job.py",
    "whale_job.py",
    # "arbitrage_job.py",
    # "orderbook_job.py",
    # "double_bottom_job.py",
]

FLINK_JOB_MANAGER = "flink-jobmanager"
FLINK_JOBS_DIR  = "/opt/flink/usrlib"
CLICKHOUSE_CONTAINER = "clickhouse"

FLINK_CONTAINERS = [
    "flink-jobmanager",
    "flink-taskmanager-1",
    "flink-taskmanager-2",
    "flink-taskmanager-3",
]

def ensure_jobs_dir():
    """Create jobs directory in container if not exists."""

    # launch flink-jobmanager and create directory inside docke (usrlib) for jobs if not exists (-p)
    # equal to: docker exec flink-jobmanager mkdir -p /opt/flink/usrlib
    result = subprocess.run(
        ["docker", "exec", FLINK_JOB_MANAGER,
         "mkdir", "-p", FLINK_JOBS_DIR],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        logger.info(f"Jobs directory ready: {FLINK_JOBS_DIR}")
    else:
        raise RuntimeError(
            f"Failed to create jobs dir: {result.stderr}"
        )

def copy_job(job_file: str):
    """Copy job file from local to Flink container."""
    # create absolute path to job file
    local_path = os.path.join(
        os.path.dirname(__file__),
        job_file
    )
    container_path = f"{FLINK_JOBS_DIR}/{job_file}"

    # copy job file to container using docker cp
    # docker cp /project/flink_jobs/{job_file} flink-jobmanager:/opt/flink/usrlib/{job_file}
    result = subprocess.run(
        ["docker", "cp", local_path,
         f"{FLINK_JOB_MANAGER}:{container_path}"],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        logger.info(f"Copied {job_file} → {container_path}")
    else:
        raise RuntimeError(
            f"Copy failed for {job_file}: {result.stderr}"
        )

def submit_job(job_file: str):
    """Submit Flink job in detached mode."""
    container_path = f"{FLINK_JOBS_DIR}/{job_file}"

    # run job in detached mode (-d) using docker exec not to block main thread
    # equal to: docker exec flink-jobmanager flink run -py /opt/flink/usrlib/{job_file} -d
    result = subprocess.run(
        [
            "docker", "exec", FLINK_JOB_MANAGER,
            "flink", "run",
            "-py", container_path,
            "--pyFiles", FLINK_JOBS_DIR,
            "-d",
        ],
        capture_output=True,
        text=True
    )

    output = result.stdout.strip() or result.stderr.strip()

    if result.returncode == 0:
        logger.info(f"Job submitted: {job_file}\n{output}")
    else:
        raise RuntimeError(
            f"Submit failed for {job_file}: {output}"
        )

def copy_dir(dir_name: str):
    """Copy directory to ALL Flink containers."""
    local_path = os.path.join(
        os.path.dirname(__file__),
        dir_name
    )

    for container in FLINK_CONTAINERS:
        # ensure target directory exists in container
        subprocess.run(
            ["docker", "exec", container,
             "mkdir", "-p", FLINK_JOBS_DIR],
            capture_output=True,
            text=True
        )

        result = subprocess.run(
            ["docker", "cp", local_path,
             f"{container}:{FLINK_JOBS_DIR}"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            logger.info(f"Copied '{dir_name}' to {container}")
        else:
            raise RuntimeError(
                f"Failed to copy to {container}: {result.stderr}"
            )

def configure_clickhouse():
    """
    Allow ClickHouse default user to connect from any IP.
    Required for Flink TaskManagers to write to ClickHouse.
    By default ClickHouse only allows localhost connections.
    """
    config = """<clickhouse>
                <users>
                    <default>
                    <networks>
                        <ip>::/0</ip>
                    </networks>
                    </default>
                </users>
                </clickhouse>"""

    # write config
    result = subprocess.run(
        ["docker", "exec", CLICKHOUSE_CONTAINER,
         "sh", "-c",
         f'cat > /etc/clickhouse-server/users.d/default-user.xml << \'EOF\'\n{config}\nEOF'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to write ClickHouse config: {result.stderr}"
        )

    # reload config
    result = subprocess.run(
        ["docker", "exec", CLICKHOUSE_CONTAINER,
         "clickhouse-client", "--query", "SYSTEM RELOAD CONFIG"],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        logger.info("ClickHouse config reloaded")
    else:
        raise RuntimeError(
            f"Failed to reload ClickHouse config: {result.stderr}"
        )
        
def main():
    logger.info("Starting Flink Jobs submission...")

    # configure ClickHouse to allow remote connections
    try:
        configure_clickhouse()
    except Exception as e:
        logger.error(f"Critical: ClickHouse configuration failed: {e}")
        return

    # create jobs directory in container
    ensure_jobs_dir()

    # copy shared modules (sinks) to container before submitting jobs that depend on them
    try:
        copy_dir("sinks")
    except Exception as e:
        logger.error(f"Critical error copying shared modules: {e}")
        return

    # run each job independently
    success = 0
    failed  = 0

    for job_file in JOBS:
        try:
            logger.info(f"Submitting: {job_file}")
            copy_job(job_file)
            submit_job(job_file)
            success += 1
        except Exception as e:
            logger.error(f"Failed to submit {job_file}: {e}")
            failed += 1
            continue  # do not stop on failure, attempt to submit remaining jobs

    logger.info(
        f"Submission complete | "
        f"success={success} | failed={failed}"
    )

if __name__ == "__main__":
    main()