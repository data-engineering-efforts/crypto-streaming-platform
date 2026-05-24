from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:29092,localhost:29093,localhost:29094"
REPLICATION_FACTOR = 3
MIN_ISR = "2"

def create_topics(admin_client: AdminClient, topics: list[NewTopic]):
    futures = admin_client.create_topics(topics)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info(f"Topic created: {topic}")
        except Exception as e:
            if "already exists" in str(e):
                logger.info(f"Topic already exists: {topic}")
            else:
                logger.error(f"Failed to create topic {topic}: {e}")
                raise

def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    topics = [

        NewTopic(
            topic="raw-binance-trades",
            num_partitions=3,  # 3 partitions for 3 symbols: BTCUSDT, ETHUSDT, SOLUSDT
            replication_factor=REPLICATION_FACTOR,
            config={
                "min.insync.replicas": MIN_ISR,
                "retention.ms": "86400000",   # 24h raw data stored in Iceberg
                "compression.type": "lz4",    # latency sensitive, fast compression
                "cleanup.policy": "delete"
            }
        ),

        NewTopic(
            topic="raw-coinbase-match",
            num_partitions=3,  # 3 partitions for 3 symbols: BTC-USD, ETH-USD, SOL-USD
            replication_factor=REPLICATION_FACTOR,
            config={
                "min.insync.replicas": MIN_ISR,
                "retention.ms": "21600000",   # 6h needed for Arbitrage Job replay
                "compression.type": "lz4",    # latency sensitive, fast compression
                "cleanup.policy": "delete"
            }
        ),

        NewTopic(
            topic="raw-orderbook",
            num_partitions=3,  # 3 partitions for 3 symbols: BTCUSDT, ETHUSDT, SOLUSDT
            replication_factor=REPLICATION_FACTOR,
            config={
                "min.insync.replicas": MIN_ISR,
                "retention.ms": "21600000",   # 6h orderbook data becomes stale quickly
                "compression.type": "snappy", # high throughput with repeating price patterns
                "cleanup.policy": "delete"
            }
        ),

        NewTopic(
            topic="whale-alerts",
            num_partitions=1,  # low volume of alerts, single partition is enough
            replication_factor=REPLICATION_FACTOR,
            config={
                "min.insync.replicas": MIN_ISR,
                "retention.ms": "172800000",  # 48h buffer for ClickHouse recovery
                "compression.type": "zstd",   # not latency sensitive, better compression
                "cleanup.policy": "delete"
            }
        ),

        NewTopic(
            topic="dlq-events",
            num_partitions=1,  # low volume, single partition preserves order for debugging
            replication_factor=REPLICATION_FACTOR,
            config={
                "min.insync.replicas": MIN_ISR,
                "retention.ms": "604800000",  # 7d time to investigate and fix issues
                "compression.type": "zstd",   # not latency sensitive, save disk space
                "cleanup.policy": "delete"
            }
        ),

        NewTopic(
            topic="recon-status",
            num_partitions=1,  # one result per hour, single partition is enough
            replication_factor=REPLICATION_FACTOR,
            config={
                "min.insync.replicas": MIN_ISR,
                "retention.ms": "604800000",  # 7d audit trail for reconciliation history
                "compression.type": "zstd",   # low volume, long retention, save disk space
                "cleanup.policy": "delete"
            }
        ),

        NewTopic(
            topic="recon-alerts",
            num_partitions=1,  # fired only on mismatch, single partition is enough
            replication_factor=REPLICATION_FACTOR,
            config={
                "min.insync.replicas": MIN_ISR,
                "retention.ms": "604800000",  # 7d audit trail for mismatch history
                "compression.type": "zstd",   # low volume, long retention, save disk space
                "cleanup.policy": "delete"
            }
        ),

        NewTopic(
            topic="recon-corrections",
            num_partitions=3,  # 3 partitions for 3 symbols: BTC, ETH, SOL
            replication_factor=REPLICATION_FACTOR,
            config={
                "min.insync.replicas": MIN_ISR,
                "retention.ms": "604800000",  # 7d audit trail for corrections
                "compression.type": "zstd",   # low volume, long retention, save disk space
                "cleanup.policy": "delete"
            }
        )
    ]

    create_topics(admin, topics)
    logger.info("All topics created successfully!")

if __name__ == "__main__":
    main()