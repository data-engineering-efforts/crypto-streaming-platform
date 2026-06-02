import os

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-1:9092,kafka-2:9092,kafka-3:9092"
)
SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL",
    "http://schema-registry:8081"
)

# ClickHouse
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = "default"

# Reconciliation
DIFF_THRESHOLD_PCT = 0.2

# Iceberg
ICEBERG_TABLES = [
    "crypto.binance_trades_raw",
    "crypto.coinbase_trades_raw",
]

NESSIE_CATALOG_PROPERTIES = ",\n        ".join([
    "'type' = 'iceberg'",
    "'catalog-impl' = 'org.apache.iceberg.nessie.NessieCatalog'",
    "'uri' = 'http://nessie:19120/api/v1'",
    "'ref' = 'main'",
    "'warehouse' = 's3://warehouse/'",
    "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'",
    "'s3.endpoint' = 'http://minio:9000'",
    "'s3.access-key-id' = 'minioadmin'",
    "'s3.secret-access-key' = 'minioadmin'",
    "'s3.path-style-access' = 'true'",
    "'s3.region' = 'us-east-1'",
    "'client.region' = 'us-east-1'",
    "'s3.endpoint-override' = 'http://minio:9000'",
])
