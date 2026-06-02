import logging
import clickhouse_driver

logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = ""

def get_client() -> clickhouse_driver.Client:
    """Create ClickHouse client."""
    return clickhouse_driver.Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

def get_recon_results(window_start) -> list:
    """Get reconciliation results from ClickHouse."""
    client = get_client()
    rows = client.execute("""
        SELECT symbol, streaming_vwap, batch_vwap, diff_pct, status
        FROM recon_results
        WHERE run_time >= now() - INTERVAL 10 MINUTE
        ORDER BY diff_pct DESC
    """)
    client.disconnect()
    return rows