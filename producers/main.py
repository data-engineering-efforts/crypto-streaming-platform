# producers/main.py
import asyncio
import logging
import os
from dotenv import load_dotenv
from pathlib import Path

from binance_producer import BinanceProducer
from coinbase_producer import CoinbaseProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Load .env from project root
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092,localhost:29093,localhost:29094"
)
SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL",
    "http://localhost:8081"
)

async def main():
    binance = BinanceProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        schema_registry_url=SCHEMA_REGISTRY_URL,
    )
    coinbase = CoinbaseProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        schema_registry_url=SCHEMA_REGISTRY_URL,
    )

    try:
        await asyncio.gather(
            binance.start(),
            coinbase.start(),
        )
    except asyncio.CancelledError:
        logger.info("Tasks cancelled.")
        raise  
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Flushing producers before shutdown...")
        binance.flush()
        coinbase.flush()
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down producers...")