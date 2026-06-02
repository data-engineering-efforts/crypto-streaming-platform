"""
Contract tests for the Avro schemas: they must parse, and a producer dict
must round-trip through serialize/deserialize without losing data — in
particular the Decimal logical types for price/quantity.
"""
from decimal import Decimal
from pathlib import Path
import io

import pytest

fastavro = pytest.importorskip("fastavro")

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "producers" / "schemas"


def _load(name):
    import json
    with open(SCHEMA_DIR / name) as f:
        return fastavro.parse_schema(json.load(f))


def test_binance_schema_parses():
    schema = _load("binance_trade.avsc")
    assert schema["name"].endswith("BinanceTrade")


def test_coinbase_schema_parses():
    # contract test: the schema file is valid Avro
    _load("coinbase_match.avsc")


def test_binance_record_roundtrips_with_decimal_precision():
    schema = _load("binance_trade.avsc")
    record = {
        "event_type": "trade",
        "event_time": 1780356266000,
        "symbol": "BTCUSDT",
        "trade_id": 12345,
        "price": Decimal("67000.12345678"),
        "quantity": Decimal("0.50000000"),
        "trade_time": 1780356265999,
        "is_buyer_maker": False,
        "ingestion_time": 1780356266050,
    }
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    buf.seek(0)
    out = fastavro.schemaless_reader(buf, schema)

    assert out["symbol"] == "BTCUSDT"
    assert out["price"] == Decimal("67000.12345678")  # no precision loss
    assert out["quantity"] == Decimal("0.50000000")
    assert out["is_buyer_maker"] is False
