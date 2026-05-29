import logging
from datetime import datetime
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
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

class DoubleBottomClickHouseSink(ClickHouseSink):
    """Double Bottom pattern sink."""

    @property
    def table_name(self) -> str:
        return "double_bottom_signals"
    
    @property
    def columns(self) -> list:
        return [
            "symbol", "first_dip", "second_dip",
            "peak_price", "breakout_price", "confirmed_at",
            "version"
        ]

    def to_record(self, value) -> tuple:
        confirmed_at = datetime.strptime(value[5][:19], "%Y-%m-%d %H:%M:%S")
        return (
            value[0], # symbol
            value[1], # first_dip
            value[2], # second_dip
            value[3], # peak_price
            value[4], # breakout_price
            confirmed_at, # confirmed_at = время BREAK
            int(confirmed_at.timestamp()), # version = unix timestamp BREAK
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
        CREATE TABLE IF NOT EXISTS binance_trades_db_pattern (
            event_type STRING,
            event_time BIGINT,
            symbol STRING,
            trade_id BIGINT,
            price DECIMAL(18, 8),
            quantity DECIMAL(18, 8),
            trade_time BIGINT,
            is_buyer_maker BOOLEAN,
            ingestion_time BIGINT,
            price_d AS CAST(price AS DOUBLE),
            trade_time_ts AS TO_TIMESTAMP_LTZ(trade_time, 3),
            WATERMARK FOR trade_time_ts AS trade_time_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw-binance-trades',
            'properties.bootstrap.servers' = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
            'properties.group.id' = 'flink-double-bottom-consumer',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'avro-confluent',
            'avro-confluent.url' = 'http://schema-registry:8081'
        )
    """)

    result_table = t_env.sql_query("""
        SELECT *
        FROM binance_trades_db_pattern
        MATCH_RECOGNIZE (
            PARTITION BY symbol
            ORDER BY trade_time_ts
            MEASURES
                FIRST(FIRST_DIP.price_d)                  AS first_dip,
                LAST(SECOND_DIP.price_d)                  AS second_dip,
                LAST(PEAK.price_d)                        AS peak_price,
                LAST(BREAK.price_d)                       AS breakout_price,
                CAST(LAST(BREAK.trade_time_ts) AS STRING) AS confirmed_at
            ONE ROW PER MATCH
            PATTERN (FIRST_DIP+ PEAK+ SECOND_DIP+ BREAK)
            WITHIN INTERVAL '10' MINUTE
            DEFINE
                FIRST_DIP  AS price_d < PREV(price_d, 1),
                PEAK       AS price_d > PREV(price_d, 1),
                SECOND_DIP AS price_d < PREV(price_d, 1),
                BREAK      AS price_d > FIRST(PEAK.price_d)
        )
    """)

    # pass one record at a time
    result_stream = t_env.to_append_stream(
        result_table,
        Types.ROW([
            Types.STRING(), # symbol
            Types.DOUBLE(), # first_dip
            Types.DOUBLE(), # second_dip
            Types.DOUBLE(), # peak_price
            Types.DOUBLE(), # breakout_price 
            Types.STRING(), # confirmed_at (STRING)
        ])
    )

    # Sink (Flink -> ClickHouse), values are inserted one by one in map() method of DoubleBottomClickHouseSink
    # parameter value is a row from result_table
    result_stream.map(
        DoubleBottomClickHouseSink(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )
    ).set_parallelism(1)

    env.execute("Double Bottom Pattern Job")

if __name__ == "__main__":
    main()