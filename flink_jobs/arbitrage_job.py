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
    CLICKHOUSE_PASSWORD,
)

logger = logging.getLogger(__name__)


class ArbitrageClickHouseSink(ClickHouseSink):
    """Arbitrage signals sink."""

    @property
    def table_name(self) -> str:
        return "arbitrage_signals"

    @property
    def columns(self) -> list:
        return [
            "symbol", "binance_price", "coinbase_price",
            "spread", "spread_pct", "direction",
            "event_time", "version"
        ]

    def to_record(self, value) -> tuple:
        event_time = datetime.strptime(value[6][:19], "%Y-%m-%d %H:%M:%S")
        return (
            value[0],  # symbol
            value[1],  # binance_price
            value[2],  # coinbase_price
            value[3],  # spread
            value[4],  # spread_pct
            value[5],  # direction
            event_time,  # event_time
            int(event_time.timestamp()),  # version
        )


def main():
    # Environment Setup
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)
    env.enable_checkpointing(60000)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.exec.source.idle-timeout", "5000ms")

    # Source Table: Binance (Kafka -> Flink)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS binance_trades_arb (
            symbol STRING,
            price DECIMAL(18, 8),
            trade_time BIGINT,
            trade_time_ts AS TO_TIMESTAMP_LTZ(trade_time, 3),
            WATERMARK FOR trade_time_ts AS trade_time_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw-binance-trades',
            'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
            'properties.group.id' = 'flink-arbitrage-binance-consumer',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'avro-confluent',
            'avro-confluent.url' = 'http://schema-registry:8081'
        )
    """)

    # Source Table: Coinbase (Kafka -> Flink)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS coinbase_trades_arb (
            product_id STRING,
            price DECIMAL(18, 8),
            event_time BIGINT,
            event_time_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR event_time_ts AS event_time_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw-coinbase-match',
            'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
            'properties.group.id' = 'flink-arbitrage-coinbase-consumer',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'avro-confluent',
            'avro-confluent.url' = 'http://schema-registry:8081'
        )
    """)

    # Interval JOIN: join events within +/-10 seconds
    # Only emit when spread > 0.1%
    result_table = t_env.sql_query("""
        SELECT
            b.symbol,
            CAST(b.price AS DOUBLE) AS binance_price,
            CAST(c.price AS DOUBLE) AS coinbase_price,
            ABS(CAST(b.price AS DOUBLE) - CAST(c.price AS DOUBLE)) AS spread,
            (ABS(CAST(b.price AS DOUBLE) - CAST(c.price AS DOUBLE))
                / CAST(b.price AS DOUBLE)) * 100 AS spread_pct,
            CASE
                WHEN CAST(b.price AS DOUBLE) > CAST(c.price AS DOUBLE)
                THEN 'BUY_COINBASE_SELL_BINANCE'
                ELSE 'BUY_BINANCE_SELL_COINBASE'
            END AS direction,
            CAST(b.trade_time_ts AS STRING) AS event_time
        FROM binance_trades_arb b
        JOIN coinbase_trades_arb c
            ON b.symbol = REPLACE(c.product_id, '-USD', 'USDT')
            AND c.event_time_ts BETWEEN
                b.trade_time_ts - INTERVAL '10' SECOND
                AND b.trade_time_ts + INTERVAL '10' SECOND
        WHERE
            (ABS(CAST(b.price AS DOUBLE) - CAST(c.price AS DOUBLE))
                / CAST(b.price AS DOUBLE)) * 100 > 0.1
    """)

    result_stream = t_env.to_append_stream(
        result_table,
        Types.ROW([
            Types.STRING(), # symbol
            Types.DOUBLE(), # binance_price
            Types.DOUBLE(), # coinbase_price
            Types.DOUBLE(), # spread
            Types.DOUBLE(), # spread_pct
            Types.STRING(), # direction
            Types.STRING(), # event_time
        ])
    )

    # Sink (Flink -> ClickHouse), values are inserted one by one in map() method of ArbitrageClickHouseSink
    # parameter value is a row from result_table
    result_stream.map(
        ArbitrageClickHouseSink(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )
    ).set_parallelism(3)

    env.execute("Arbitrage Monitor Job")

if __name__ == "__main__":
    main()