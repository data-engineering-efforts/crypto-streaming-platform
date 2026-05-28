import logging
from datetime import datetime
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, MapFunction
from pyflink.table import StreamTableEnvironment

from sinks.clickhouse_sink import (
    ClickHouseSink,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_DB,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD
)

logger = logging.getLogger(__name__)

class WhaleClickHouseSink(ClickHouseSink):
    """Whale aggregation sink."""

    @property
    def table_name(self) -> str:
        return "whale_alerts"

    @property
    def columns(self) -> list:
        return [
            "symbol", "price", "quantity", "trade_value",
            "exchange", "side", "event_time", "version"
        ]

    def to_record(self, value) -> tuple:
        event_time = datetime.strptime(value[6][:19], "%Y-%m-%d %H:%M:%S")
        return (
            value[0], # symbol
            value[1], # price
            value[2], # quantity
            value[3], # trade_value
            value[4], # exchange
            value[5], # side
            event_time, # event_time
            int(event_time.timestamp()) # version
        )

def main():
    # Environment Setup

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # low volume of whale alerts, no need for parallelism
    env.enable_checkpointing(60000)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.exec.source.idle-timeout", "5000ms")
    #t_env.get_config().set(
    #"table.exec.legacy-cast-behaviour", "enabled"
    #)

    # Source Table for Binance (Kafka -> Flink)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS binance_trades_whale(
            event_type STRING,
            event_time BIGINT,
            symbol STRING,
            trade_id BIGINT,
            price DECIMAL(18, 8),
            quantity DECIMAL(18, 8),
            trade_time BIGINT,
            is_buyer_maker BOOLEAN,
            ingestion_time BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw-binance-trades',
            'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
            'properties.group.id' = 'flink-whale-binance-consumer',
            'format' = 'avro-confluent',
            'scan.startup.mode' = 'latest-offset',
            'avro-confluent.url' = 'http://schema-registry:8081'
        )
    """)

    # Source Table for Coinbase (Kafka -> Flink)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS coinbase_trades_whale(
            trade_id STRING,
            product_id STRING,
            price DECIMAL(18, 8),
            size DECIMAL(18, 8),
            side STRING,
            event_time BIGINT,
            ingestion_time BIGINT
        ) WITH (
            'connector'= 'kafka',
            'topic' = 'raw-coinbase-match',
            'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
            'properties.group.id' = 'flink-whale-coinbase-consumer',
            'format' = 'avro-confluent',
            'scan.startup.mode' = 'latest-offset',
            'avro-confluent.url' = 'http://schema-registry:8081'
        )
    """)

    result_table = t_env.sql_query("""
            SELECT symbol, price, quantity, trade_value, exchange, side, event_time
            FROM (
                SELECT 
                    symbol, 
                    CAST(price AS DOUBLE) AS price, 
                    CAST(quantity AS DOUBLE) AS quantity,
                    CAST(price AS DOUBLE) * CAST(quantity AS DOUBLE) AS trade_value,
                    'binance' AS exchange,
                    CASE WHEN is_buyer_maker THEN 'SELL' ELSE 'BUY' END AS side,
                    CAST(TO_TIMESTAMP_LTZ(trade_time, 3) AS STRING) AS event_time
                FROM binance_trades_whale

                UNION ALL

                SELECT 
                    product_id AS symbol, 
                    CAST(price AS DOUBLE) AS price, 
                    CAST(size AS DOUBLE) AS quantity,
                    CAST(price AS DOUBLE) * CAST(size AS DOUBLE) AS trade_value,
                    'coinbase' AS exchange,
                    CAST(side AS STRING) AS side,
                    CAST(TO_TIMESTAMP_LTZ(event_time, 3) AS STRING) AS event_time
                FROM coinbase_trades_whale
            )
            WHERE 
                (symbol = 'BTCUSDT' AND quantity >= 5.0)
                OR (symbol = 'ETHUSDT' AND quantity >= 50.0)
                OR (symbol = 'SOLUSDT' AND quantity >= 500.0)
    """)

    # pass one record at a time
    result_stream = t_env.to_append_stream(
        result_table,
        Types.ROW([
            Types.STRING(), # symbol
            Types.DOUBLE(), # price
            Types.DOUBLE(), # quantity
            Types.DOUBLE(), # trade_value
            Types.STRING(), # exchange
            Types.STRING(), # side
            Types.STRING(), # event_time
        ])
    )

    # Sink (Flink -> ClickHouse), values are inserted one by one in map() method of ClickHouseSink
    # parameter value is a row from result_table
    result_stream.map(
        WhaleClickHouseSink(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )
    ).set_parallelism(1) # single connection to ClickHouse for simplicity, not a bottleneck for low volume of whale alerts

    env.execute("Whale Aggregation Job")

if __name__ == "__main__":
    main()