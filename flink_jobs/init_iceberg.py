from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    t_env = TableEnvironment.create(
        EnvironmentSettings.in_streaming_mode()
    )

    # create Nessie catalog
    print("Creating Nessie catalog...")
    t_env.execute_sql("""
        CREATE CATALOG nessie_catalog WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'org.apache.iceberg.nessie.NessieCatalog',
            'uri' = 'http://nessie:19120/api/v1',
            'ref' = 'main',
            'warehouse' = 's3://warehouse/',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = 'http://minio:9000',
            's3.access-key-id' = 'minioadmin',
            's3.secret-access-key' = 'minioadmin',
            's3.path-style-access' = 'true',
            's3.region' = 'us-east-1',
            'client.region' = 'us-east-1',
            's3.endpoint-override' = 'http://minio:9000'
        )
    """)

    t_env.execute_sql("USE CATALOG nessie_catalog")
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS crypto")
    t_env.execute_sql("USE crypto")

    # raw Binance trades for Reconciliation
    print("Creating binance_trades_raw...")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS binance_trades_raw (
            symbol         STRING,
            price          DOUBLE,
            quantity       DOUBLE,
            trade_time     BIGINT,
            is_buyer_maker BOOLEAN,
            ingestion_time BIGINT
        )
    """)

    # raw Coinbase trades for Reconciliation
    print("Creating coinbase_trades_raw...")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS coinbase_trades_raw (
            trade_id       STRING,
            product_id     STRING,
            price          DOUBLE,
            size           DOUBLE,
            side           STRING,
            event_time     BIGINT,
            ingestion_time BIGINT
        )
    """)

    print("Iceberg tables initialized successfully")

if __name__ == "__main__":
    main()