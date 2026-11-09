# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A local, Docker Compose–based real-time crypto analytics platform (portfolio project): Binance/Coinbase WebSocket → Python producers (Avro via Schema Registry) → Kafka (3 KRaft brokers) → PyFlink jobs → ClickHouse (results, Grafana dashboards) and MinIO + Iceberg + Nessie (raw-trade lake). An hourly Airflow DAG re-computes VWAP from Iceberg and compares it to the streaming VWAP in ClickHouse. README.md has the full description, known limitations, and the "Engineering notes" list of past bugs — read those before touching time handling, the ClickHouse sink, WebSocket settings, or watermarks.

## Commands

```bash
python -m venv venv && source venv/bin/activate && pip install -r requirements.txt
cp .env.example .env

make up          # infra (docker compose up + wait for kafka-1..3, clickhouse, minio healthy) -> topics -> jobs
make producers   # run in a second terminal; foreground, long-running
make infra | make topics | make jobs   # individual bring-up steps
make down        # stop, keep volumes
make clean       # stop AND delete volumes (wipes all data)
make logs / make ps

make test                                              # pytest (testpaths=tests, -v --tb=short)
pytest tests/test_binance_producer.py::test_parse_trade_uses_decimal_not_float   # single test
```

Docker needs ~16 GB memory; less makes brokers unstable. Host Python is 3.11 (`.python-version`).

## Architecture notes that span multiple files

**Host vs container execution.** Producers, `scripts/create_topics.py`, and `flink_jobs/main.py` run on the host. Host code reaches Kafka at `localhost:29092-29094` and Schema Registry at `localhost:8081` (from `.env`); code running inside containers uses the in-network names in `flink_jobs/shared/config.py` (`kafka-1:9092`, `clickhouse:9000`, `nessie`, `minio`, etc.).

**How Flink jobs get deployed.** Job files are not baked into the image. `flink_jobs/main.py` (`make jobs`) uses `docker cp`/`docker exec` to: copy `shared/` into `/opt/flink/usrlib` on the JobManager *and both TaskManagers*, run `init_iceberg.py` in the JobManager to create Iceberg tables, then copy and `flink run -py ... --pyFiles /opt/flink/usrlib -d` each job in `JOBS`. After editing a job or `shared/`, re-run `make jobs` (cancel the old job in the Flink UI first, or it will run twice). New jobs must be added to the `JOBS` list.

**Imports are flat, not package-relative.** Producers use `from base_producer import ...`; Flink jobs use `from shared.config import ...`. This works because each runs with its own directory as the root (`cd producers`, `/opt/flink/usrlib` on PYTHONPATH). `tests/conftest.py` adds `producers/` and `flink_jobs/` to `sys.path` to mimic this. Keep new modules consistent with that.

**Producers.** `producers/base_producer.py` holds the shared Kafka/Avro/DLQ logic; exchange subclasses implement parsing and `to_dict`. Avro schemas are in `producers/schemas/*.avsc`. Prices/quantities are `Decimal`, not float. `SYMBOL_PARTITIONS` pins BTC/ETH/SOL to partitions 0/1/2 on purpose so no partition stays idle (idle partitions stall event-time windows). Topic auto-creation is disabled — new topics go in `scripts/create_topics.py`.

**Flink jobs** (`flink_jobs/*_job.py`) mostly define sources/logic with Flink SQL via `StreamTableEnvironment.execute_sql` (the double-bottom job uses `MATCH_RECOGNIZE`), then write to ClickHouse through a subclass of `shared/clickhouse_sink.ClickHouseSink`. That sink is a `MapFunction` that micro-batches in a Python list (PyFlink has no `RichSinkFunction`), so delivery is at-least-once. Subclasses define `table_name`, `columns`, and `to_record(value) -> tuple`; column order must match `clickhouse/init/001_create_tables.sql`.

**ClickHouse dedup.** All result tables are `ReplacingMergeTree(version)`. `version` must be derived from UTC-aware datetimes (`.replace(tzinfo=timezone.utc)` before `.timestamp()`), otherwise it differs between machines and dedup silently breaks. Dashboard/reading queries must use `FINAL`.

**Reconciliation / Airflow.** `airflow/dags/reconciliation_dag.py` doesn't run Spark or Flink-native submission: `airflow/operators/flink_operator.py` `docker cp`s a script from the mounted `/opt/airflow/flink_jobs` into the Flink JobManager and runs it with `python3` (PYTHONPATH `/opt/flink/usrlib`, so `shared/` must already be copied by `make jobs`). `check_iceberg_data.py` must print the row count as its last stdout line — the DAG parses it. The 2-hour window is duplicated across the DAG, `check_iceberg_data.py`, and `reconciliation_job.py`; change them together. Alert threshold is `DIFF_THRESHOLD_PCT` in `shared/config.py`. `iceberg_maintenance_dag.py` is an intentional placeholder (PyIceberg can't maintain Flink/Nessie-written tables).

**Monitoring.** `monitoring/prometheus.yml` + `alerts.yml` (lag alerts use `for: 15m`; the `consumergroup_members == 0` alert is intentionally disabled because Flink manages its own offsets). Grafana dashboards and the ClickHouse datasource are provisioned from `monitoring/grafana/`.

## Tests

Tests that need `pyflink`, `clickhouse_driver`, or `confluent_kafka` are skipped via `pytest.importorskip` when those aren't installed (`pyflink` and `clickhouse_driver` are not in the root `requirements.txt`, so `test_transforms.py` skips on a plain host venv), so the Avro schema tests always run. Tests don't need the Docker stack.
