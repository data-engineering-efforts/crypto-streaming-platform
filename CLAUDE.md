# Crypto Streaming Platform

## Project Goal
Production-grade real-time crypto analytics platform for FAANG portfolio.
Processes Binance + Coinbase market data through Kafka/Flink pipeline.

## Architecture
- Binance + Coinbase WebSocket → 2 Kafka Producers → Kafka 1 → Flink → ClickHouse
- Schema Registry (Avro) — one instance, two schemas
- MinIO/Iceberg — raw data + Flink checkpoints
- Kafka 2 — Reconciliation topics only
- Airflow — triggers Flink Batch Job (Reconciliation) via REST API
- Grafana + Prometheus — observability

## Flink Jobs
- Job 1: VWAP Aggregation (tumbling windows 1m/5m)
- Job 2: Whale Detector (trades > 50 BTC)
- Job 3: Arbitrage Monitor (Binance vs Coinbase stream-stream join)
- Job 4: Order Book Imbalance (buy/sell pressure ratio)
- Job 5: Double Bottom CEP (MATCH_RECOGNIZE bullish signal)
- Job 6: Reconciliation Batch (triggered by Airflow hourly)

## Kafka Topics
### Kafka 1 (Streaming)
- raw-binance-trades
- raw-coinbase-match
- raw-orderbook
- whale-alerts
- dlq-events

### Kafka 2 (Reconciliation)
- recon-status
- recon-alerts
- recon-corrections

## Tech Stack
- Python 3.11 — Kafka Producers
- PyFlink 1.19 — Stream + Batch processing
- Apache Kafka 3.7 (KRaft, no ZooKeeper)
- Confluent Schema Registry 7.6
- Apache Iceberg — table format on MinIO
- MinIO — S3-compatible local storage
- ClickHouse 24.3 — OLAP sink (ReplacingMergeTree)
- Apache Airflow 2.9 — orchestration
- Grafana 10.4 + Prometheus — monitoring
- Docker Compose — local dev (M1 Pro arm64)

## Key Architecture Decisions
- Partition key = symbol (stateful Flink ops on same partition)
- Exactly-once: idempotent producer + Flink checkpointing + ReplacingMergeTree
- Kappa Architecture: one Flink cluster for stream and batch
- Two producers: BinanceProducer, CoinbaseProducer (different schemas)
- Flink writes directly to ClickHouse via JDBC Sink (no Kafka Connector)
- Reconciliation writes delta only (not raw events) to Kafka 2

## Project Structure
producers/          — Kafka Producers (Binance, Coinbase)
producers/schemas/  — Avro schemas (.avsc files)
flink_jobs/         — PyFlink jobs (6 jobs)
airflow/dags/       — Reconciliation DAG
clickhouse/init/    — ClickHouse table DDL
monitoring/         — Prometheus config + Grafana dashboards
docker/             — Flink custom Dockerfile

## Running Locally
docker compose up -d
python producers/binance_producer.py
python producers/coinbase_producer.py
