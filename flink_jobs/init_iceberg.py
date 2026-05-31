import logging
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment
from shared.config import NESSIE_CATALOG_PROPERTIES

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    t_env = TableEnvironment.create(
        EnvironmentSettings.in_streaming_mode()
    )

    # create Nessie catalog
    logger.info("Creating Nessie catalog...")
    t_env.execute_sql(f"""
        CREATE CATALOG nessie_catalog WITH (
            {NESSIE_CATALOG_PROPERTIES}
        )
    """)

    t_env.execute_sql("USE CATALOG nessie_catalog")
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS crypto")
    t_env.execute_sql("USE crypto")

    # raw Binance trades for Reconciliation
    logger.info("Creating binance_trades_raw...")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS binance_trades_raw (
            symbol STRING,
            price DOUBLE,
            quantity DOUBLE,
            trade_time BIGINT,
            is_buyer_maker BOOLEAN,
            ingestion_time BIGINT
        )
    """)

    # raw Coinbase trades for Reconciliation
    logger.info("Creating coinbase_trades_raw...")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS coinbase_trades_raw (
            trade_id STRING,
            product_id STRING,
            price DOUBLE,
            size DOUBLE,
            side STRING,
            event_time BIGINT,
            ingestion_time BIGINT
        )
    """)

    logger.info("Iceberg tables initialized successfully")

if __name__ == "__main__":
    main()