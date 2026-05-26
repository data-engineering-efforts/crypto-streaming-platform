from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)
    env.enable_checkpointing(60000)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.exec.source.idle-timeout", "5000ms")

    # Source Table (Kafka -> Flink)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS binance_trades (
            event_type      STRING,
            event_time      BIGINT,
            symbol          STRING,
            trade_id        BIGINT,
            price           DECIMAL(18, 8),
            quantity        DECIMAL(18, 8),
            trade_time      BIGINT,
            is_buyer_maker  BOOLEAN,
            ingestion_time  BIGINT,
            trade_time_ts   AS TO_TIMESTAMP_LTZ(trade_time, 3),
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

    #Sink Table (print для проверки)
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS vwap_sink (
            symbol         STRING,
            window_start   TIMESTAMP(3),
            window_end     TIMESTAMP(3),
            vwap           DOUBLE,
            total_volume   DOUBLE,
            trade_count    BIGINT,
            interval_label STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    # VWAP Query
    t_env.execute_sql("""
        INSERT INTO vwap_sink
        SELECT
            symbol,
            TUMBLE_START(trade_time_ts, INTERVAL '5' MINUTE) AS window_start,
            TUMBLE_END(trade_time_ts, INTERVAL '5' MINUTE) AS window_end,
            SUM(CAST(price AS DOUBLE) * CAST(quantity AS DOUBLE))
                / SUM(CAST(quantity AS DOUBLE)) AS vwap,
            SUM(CAST(quantity AS DOUBLE)) AS total_volume,
            COUNT(trade_id) AS trade_count,
            '5m' AS interval_label
        FROM binance_trades
        GROUP BY
            symbol,
            TUMBLE(trade_time_ts, INTERVAL '5' MINUTE)
    """).wait()


if __name__ == "__main__":
    main()