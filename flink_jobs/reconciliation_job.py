import logging
import sys

from datetime import datetime, timezone, timedelta
import clickhouse_driver
from pyflink.table import EnvironmentSettings, TableEnvironment

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

from shared.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    NESSIE_CATALOG_PROPERTIES,
    DIFF_THRESHOLD_PCT
)

def get_time_window():
    """
    Get the last completed hour window for reconciliation.
    Example: if now is 17:35, returns (16:00, 17:00)
    """
    now = datetime.now(timezone.utc)
    window_end = now.replace(minute=0, second=0, microsecond=0)
    window_start = window_end - timedelta(hours=1)
    return window_start, window_end

def compute_batch_vwap(t_env, window_start: datetime, window_end: datetime) -> dict:
    """
    Read raw trades from Iceberg and compute VWAP in batch mode.
    Returns dict: {symbol: batch_vwap}
    """
    window_start_ms = int(window_start.timestamp() * 1000)
    window_end_ms = int(window_end.timestamp() * 1000)

    result = t_env.execute_sql(f"""
        SELECT
            symbol,
            COALESCE(SUM(price * quantity) / SUM(quantity), 0.0) AS batch_vwap
        FROM nessie_catalog.crypto.binance_trades_raw
        WHERE trade_time >= {window_start_ms}
          AND trade_time < {window_end_ms}
        GROUP BY symbol
    """)

    batch_vwap = {}

    # only one row per symbol, so we can collect results directly
    with result.collect() as rows:
        for row in rows:
            symbol = row[0]
            vwap_value = row[1]
            batch_vwap[symbol] = vwap_value
            logger.info(f"Batch VWAP: {symbol} = {vwap_value:.4f}")

    return batch_vwap

def get_streaming_vwap(window_start: datetime, window_end: datetime) -> dict:
    """
    Read VWAP from ClickHouse (computed by streaming Job).
    Returns dict: {symbol: streaming_vwap}
    """
    client = clickhouse_driver.Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

    rows = client.execute("""
        SELECT
            symbol,
            SUM(vwap * total_volume) / SUM(total_volume) AS streaming_vwap
        FROM vwap_aggregations
        WHERE window_start >= %(start)s
          AND window_end <= %(end)s
        GROUP BY symbol
    """, {
        "start": window_start,
        "end":   window_end,
    })

    streaming_vwap = {}
    for row in rows:
        symbol  = row[0]
        vwap_value = row[1]
        streaming_vwap[symbol] = vwap_value
        logger.info(f"Streaming VWAP: {symbol} = {vwap_value:.4f}")

    client.disconnect()
    return streaming_vwap

def save_results(results: list):
    """Write reconciliation results to ClickHouse recon_results table."""
    client = clickhouse_driver.Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

    client.execute(
        """
        INSERT INTO recon_results
            (run_time, window_start, window_end, symbol,
             streaming_vwap, batch_vwap, diff_pct, status, version)
        VALUES
        """,
        results,
    )

    client.disconnect()
    logger.info(f"Saved {len(results)} recon results to ClickHouse")

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )

    t_env = TableEnvironment.create(
        EnvironmentSettings.in_batch_mode()
    )

    t_env.execute_sql(f"""
        CREATE CATALOG nessie_catalog WITH (
            {NESSIE_CATALOG_PROPERTIES}
        )
    """)

    window_start, window_end = get_time_window()
    logger.info(f"Reconciliation window: {window_start} - {window_end}")

    logger.info("Computing batch VWAP from Iceberg...")
    batch_vwap = compute_batch_vwap(t_env, window_start, window_end)

    if not batch_vwap:
        logger.warning("No data in Iceberg for this windows. Skipping reconciliation.")
        return

    logger.info("Reading streaming VWAP from ClickHouse...")
    streaming_vwap = get_streaming_vwap(window_start, window_end)

    if not streaming_vwap:
        logger.warning("No data in ClickHouse for this window. Skipping reconciliation.")
        return

    run_time = datetime.now(timezone.utc)
    results = []

    all_symbols = set(batch_vwap.keys()) | set(streaming_vwap.keys())

    for symbol in all_symbols:
        b_vwap = batch_vwap.get(symbol, 0.0)
        s_vwap = streaming_vwap.get(symbol, 0.0)

        if b_vwap == 0.0 and s_vwap == 0.0:
                logger.warning(f"{symbol}: No data found in both Iceberg and ClickHouse. Skipping.")
                continue

        if b_vwap == 0.0:
            logger.error(f"{symbol}: CRITICAL | Data exists in ClickHouse ({s_vwap:.4f}) but missing in Iceberg!")
            continue

        if s_vwap == 0.0:
            logger.error(f"{symbol}: CRITICAL | Data exists in Iceberg ({b_vwap:.4f}) but missing in ClickHouse!")
            continue

        diff_pct = abs(b_vwap - s_vwap) / b_vwap * 100

        if diff_pct > DIFF_THRESHOLD_PCT:
            status = "ALERT"
            logger.error(
                f"{symbol}: ALERT! "
                f"streaming={s_vwap:.4f} batch={b_vwap:.4f} "
                f"diff={diff_pct:.4f}%"
            )
        else:
            status = "OK"
            logger.info(
                f"{symbol}: OK "
                f"streaming={s_vwap:.4f} batch={b_vwap:.4f} "
                f"diff={diff_pct:.4f}%"
            )

        results.append((
            run_time,
            window_start,
            window_end,
            symbol,
            s_vwap,
            b_vwap,
            diff_pct,
            status,
            int(run_time.timestamp()),
        ))

    if results:
        save_results(results)
        logger.info("Reconciliation complete")

if __name__ == "__main__":
    main()