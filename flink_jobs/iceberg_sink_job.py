import logging
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment

logger = logging.getLogger(__name__)

from shared.config import NESSIE_CATALOG_PROPERTIES

def main():
    # Environment Setup
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    env.enable_checkpointing(300000)  # Trigger checkpoint every 5 minutes
    ch_config = env.get_checkpoint_config()
    ch_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    ch_config.set_checkpoint_timeout(120000)        # 2 minutes timeout
    ch_config.set_min_pause_between_checkpoints(60000) # 1 minute pause between checkpoints
    ch_config.set_tolerable_checkpoint_failure_number(2)
    ch_config.set_max_concurrent_checkpoints(1)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.exec.source.idle-timeout", "5000ms")
    t_env.get_config().set("pipeline.name", "Iceberg Sink Job - Binance and Coinbase Raw Trades")

    # Nessie Catalog
    t_env.execute_sql(f"""
        CREATE CATALOG nessie_catalog WITH (
            {NESSIE_CATALOG_PROPERTIES}
        )
    """)

    # Source Table: Binance (Kafka -> Flink)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS binance_trades_source (
            event_type STRING,
            event_time BIGINT,
            symbol STRING,
            trade_id BIGINT,
            price DECIMAL(18, 8),
            quantity DECIMAL(18, 8),
            trade_time BIGINT,
            is_buyer_maker BOOLEAN,
            ingestion_time BIGINT,
            trade_time_ts AS TO_TIMESTAMP_LTZ(trade_time, 3),
            WATERMARK FOR trade_time_ts
                AS trade_time_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw-binance-trades',
            'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
            'properties.group.id' = 'flink-iceberg-binance-consumer',
            'scan.startup.mode' = 'group-offsets',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'avro-confluent',
            'avro-confluent.url' = 'http://schema-registry:8081'
        )
    """)

    # Source Table: Coinbase (Kafka -> Flink)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS coinbase_trades_source (
            trade_id STRING,
            product_id STRING,
            price DECIMAL(18, 8),
            size DECIMAL(18, 8),
            side STRING,
            event_time BIGINT,
            ingestion_time BIGINT,
            event_time_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR event_time_ts
                AS event_time_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw-coinbase-match',
            'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
            'properties.group.id' = 'flink-iceberg-coinbase-consumer',
            'scan.startup.mode' = 'group-offsets',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'avro-confluent',
            'avro-confluent.url' = 'http://schema-registry:8081'
        )
    """)

    # StatementSet allows us to execute multiple SQL statements as a single Flink Job
    stmt_set = t_env.create_statement_set()

    stmt_set.add_insert_sql("""
        INSERT INTO nessie_catalog.crypto.binance_trades_raw
        SELECT
            symbol,
            CAST(price AS DOUBLE) AS price,
            CAST(quantity AS DOUBLE) AS quantity,
            trade_time,
            is_buyer_maker,
            ingestion_time
        FROM binance_trades_source
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO nessie_catalog.crypto.coinbase_trades_raw
        SELECT
            trade_id,
            product_id,
            CAST(price AS DOUBLE) AS price,
            CAST(size AS DOUBLE) AS size,
            side,
            event_time,
            ingestion_time
        FROM coinbase_trades_source
    """)

    # run both inserts as a single Flink job
    stmt_set.execute()

if __name__ == "__main__":
    main()
