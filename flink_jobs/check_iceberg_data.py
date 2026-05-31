import logging
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment
from datetime import datetime, timedelta, timezone

from shared.config import NESSIE_CATALOG_PROPERTIES

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    t_env = TableEnvironment.create(
        EnvironmentSettings.in_batch_mode()
    )

    t_env.execute_sql(f"""
        CREATE CATALOG nessie_catalog WITH (
            {NESSIE_CATALOG_PROPERTIES}
        )
    """)

    now = datetime.now(timezone.utc)
    window_start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=2)
    window_end = now
    start_ms = int(window_start.timestamp() * 1000)
    end_ms = int(window_end.timestamp() * 1000)

    result = t_env.execute_sql(f"""
        SELECT COUNT(*)
        FROM nessie_catalog.crypto.binance_trades_raw
        WHERE trade_time >= {start_ms}
          AND trade_time <  {end_ms}
    """)

    with result.collect() as rows:
        count = next(iter(rows))[0]

    print(count)

if __name__ == "__main__":
    main()