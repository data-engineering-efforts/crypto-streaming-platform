"""
Unit tests for the Binance producer's pure transformation logic:
raw WebSocket JSON -> BinanceTrade -> Avro dict, plus whale detection.

No Kafka / Schema Registry needed: we bypass __init__ (which opens a
network connection) and exercise only the stateless methods.
"""
from decimal import Decimal
import pytest

# binance_producer transitively imports confluent_kafka; skip cleanly
# if the producer deps aren't installed in the test environment.
pytest.importorskip("confluent_kafka", reason="producer deps (confluent-kafka) not installed")

from binance_producer import BinanceProducer, WHALE_THRESHOLDS  # noqa: E402


@pytest.fixture
def producer():
    # __new__ skips __init__ (Schema Registry connection). The methods
    # under test don't read instance state, so this is safe and fast.
    return BinanceProducer.__new__(BinanceProducer)


RAW = {
    "e": "trade", "E": 1780356266000, "s": "BTCUSDT", "t": 12345,
    "p": "81.48", "q": "12.5", "T": 1780356265999, "m": False, "M": True,
}


def test_parse_trade_maps_short_field_names(producer):
    t = producer._parse_trade(RAW)
    assert t.symbol == "BTCUSDT"
    assert t.trade_id == 12345
    assert t.event_time == 1780356266000
    assert t.is_buyer_maker is False


def test_parse_trade_uses_decimal_not_float(producer):
    # price/quantity must stay Decimal — float would lose precision for money
    t = producer._parse_trade({**RAW, "p": "0.1", "q": "0.2"})
    assert isinstance(t.price, Decimal)
    assert t.price + Decimal("0.2") == Decimal("0.3")  # this fails with float math


def test_parse_trade_skips_invalid_market_trade(producer):
    # M=false marks a no market trade and must be dropped
    assert producer._parse_trade({**RAW, "M": False}) is None


def test_to_dict_emits_exactly_the_schema_fields(producer):
    t = producer._parse_trade(RAW)
    d = producer.to_dict(t, ctx=None)
    assert set(d) == {
        "event_type", "event_time", "symbol", "trade_id", "price",
        "quantity", "trade_time", "is_buyer_maker", "ingestion_time",
    }
    assert d["symbol"] == "BTCUSDT"


@pytest.mark.parametrize("is_buyer_maker, expected_side", [
    (True, "SELL"), # buyer is maker => a sell order was filled
    (False, "BUY"),
])
def test_whale_side_derivation(producer, monkeypatch, is_buyer_maker, expected_side):
    captured = {}
    monkeypatch.setattr(producer, "_log_whale", lambda **kw: captured.update(kw))
    # quantity 10 >= BTC threshold (5) => should trigger
    t = producer._parse_trade({**RAW, "q": "10", "m": is_buyer_maker})
    producer._check_whale(t)
    assert captured.get("side") == expected_side


def test_whale_below_threshold_does_not_trigger(producer, monkeypatch):
    captured = {}
    monkeypatch.setattr(producer, "_log_whale", lambda **kw: captured.update(kw))
    t = producer._parse_trade({**RAW, "q": "1"})  # 1 < BTC threshold (5)
    producer._check_whale(t)
    assert captured == {}


def test_whale_thresholds_cover_every_traded_symbol():
    assert set(WHALE_THRESHOLDS) == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
