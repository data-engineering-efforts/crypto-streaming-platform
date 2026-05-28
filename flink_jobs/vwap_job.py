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

class VwapClickHouseSink(ClickHouseSink):
    """VWAP aggregation sink."""

    @property
    def table_name(self) -> str:
        return "vwap_aggregations"

    @property
    def columns(self) -> list:
        return [
            "symbol", "window_start", "window_end",
            "vwap", "total_volume", "trade_count",
            "interval_label", "version"
        ]

    def to_record(self, value) -> tuple:
        window_start = datetime.strptime(value[1][:19], "%Y-%m-%d %H:%M:%S")
        window_end   = datetime.strptime(value[2][:19], "%Y-%m-%d %H:%M:%S")
        return (
            value[0], # symbol
            window_start, # window_start
            window_end, # window_end
            value[3], # vwap
            value[4], # total_volume
            value[5], # trade_count
            value[6], # interval_label
            int(window_start.timestamp()), # version
        )

def main():
    # Environment Setup
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)
    env.enable_checkpointing(60000)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.exec.source.idle-timeout", "5000ms")

    # Source Table (Kafka -> Flink)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS binance_trades (
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
            WATERMARK FOR trade_time_ts AS trade_time_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw-binance-trades',
            'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
            'properties.group.id' = 'flink-vwap-consumer',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'avro-confluent',
            'avro-confluent.url' = 'http://schema-registry:8081'
        )
    """)

    result_table = t_env.sql_query("""
        SELECT
            symbol,
            CAST(TUMBLE_START(trade_time_ts, INTERVAL '5' MINUTE) AS STRING) AS window_start,
            CAST(TUMBLE_END(trade_time_ts, INTERVAL '5' MINUTE) AS STRING) AS window_end,
            SUM(CAST(price AS DOUBLE) * CAST(quantity AS DOUBLE))
                / SUM(CAST(quantity AS DOUBLE)) AS vwap,
            SUM(CAST(quantity AS DOUBLE)) AS total_volume,
            COUNT(trade_id) AS trade_count,
            '5m' AS interval_label
        FROM binance_trades
        GROUP BY
            symbol,
            TUMBLE(trade_time_ts, INTERVAL '5' MINUTE)
    """)

    # pass one record at a time
    result_stream = t_env.to_append_stream(
        result_table,
        Types.ROW([
            Types.STRING(), # symbol
            Types.STRING(), # window_start (STRING)
            Types.STRING(), # window_end   (STRING)
            Types.DOUBLE(), # vwap
            Types.DOUBLE(), # total_volume
            Types.LONG(), # trade_count
            Types.STRING(), # interval_label
        ])
    )

    # Sink (Flink -> ClickHouse), values are inserted one by one in map() method of VwapClickHouseSink
    # parameter value is a row from result_table
    result_stream.map(
        VwapClickHouseSink(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )
    ).set_parallelism(1) # single connection to ClickHouse for simplicity, we have max 3 records per 5 minutes (3 symbols)

    env.execute("VWAP Aggregation Job")

if __name__ == "__main__":
    main()