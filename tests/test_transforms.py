"""
Unit tests for the ClickHouse sink to_record() transforms.

These currently import the sink classes, which pull in pyflink (the sinks
subclass a pyflink MapFunction). That's why the whole module is guarded by
importorskip. To make these tests environment-independent, extract the
to_record bodies into pure module level functions (e.g. flink_jobs/transforms.py)
and have the sinks call them  then this guard can go away.
"""
from datetime import datetime
import pytest

pytest.importorskip("pyflink", reason="to_record lives on a pyflink-coupled sink")
pytest.importorskip("clickhouse_driver", reason="shared.clickhouse_sink imports clickhouse_driver")

from whale_job import WhaleClickHouseSink          # noqa: E402
from arbitrage_job import ArbitrageClickHouseSink  # noqa: E402
from vwap_job import VwapClickHouseSink            # noqa: E402


def _sink(cls):
    # to_record doesn't touch instance state, so skip __init__ (DB connection).
    return cls.__new__(cls)


def test_whale_to_record_parses_time_and_columns_align():
    sink = _sink(WhaleClickHouseSink)
    row = ("BTCUSDT", 81.48, 12.0, 977.0, "binance", "BUY", "2026-06-01 23:24:26.123")
    rec = sink.to_record(row)
    # column order must match the declared schema
    assert len(rec) == len(sink.columns)
    assert rec[0] == "BTCUSDT"
    assert rec[6] == datetime(2026, 6, 1, 23, 24, 26)   # millis truncated by [:19]
    assert rec[7] == int(datetime(2026, 6, 1, 23, 24, 26).timestamp())  # version


def test_arbitrage_to_record_shape():
    sink = _sink(ArbitrageClickHouseSink)
    row = ("SOLUSDT", 81.48, 81.30, 0.18, 0.2209, "BUY_COINBASE_SELL_BINANCE",
           "2026-06-01 23:24:26")
    rec = sink.to_record(row)
    assert len(rec) == len(sink.columns)
    assert rec[5] == "BUY_COINBASE_SELL_BINANCE"
    assert rec[6] == datetime(2026, 6, 1, 23, 24, 26)


def test_vwap_to_record_parses_both_window_bounds():
    sink = _sink(VwapClickHouseSink)
    row = ("BTCUSDT", "2026-06-01 23:24:00", "2026-06-01 23:25:00",
           67000.0, 12.5, 42, "1m")
    rec = sink.to_record(row)
    assert len(rec) == len(sink.columns)
    assert rec[1] == datetime(2026, 6, 1, 23, 24, 0) # window_start
    assert rec[2] == datetime(2026, 6, 1, 23, 25, 0) # window_end
    assert rec[7] == int(datetime(2026, 6, 1, 23, 24, 0).timestamp()) # version


def test_to_record_truncates_subsecond_precision():
    # the [:19] slice drops fractional seconds — guard against format drift
    sink = _sink(WhaleClickHouseSink)
    rec = sink.to_record(("BTCUSDT", 1, 1, 1, "binance", "BUY",
                          "2026-06-01 23:24:26.999999"))
    assert rec[6] == datetime(2026, 6, 1, 23, 24, 26)


@pytest.mark.xfail(reason="version derives from a naive datetime, so it is "
                          "interpreted in the server's local timezone — the same "
                          "event yields different versions across timezones. "
                          "Fix: parse as UTC before .timestamp().", strict=False)
def test_version_should_be_timezone_independent():
    import calendar
    sink = _sink(WhaleClickHouseSink)
    rec = sink.to_record(("BTCUSDT", 1, 1, 1, "binance", "BUY",
                          "2026-06-01 23:24:26"))
    # expected if event_time were treated as UTC
    expected_utc = calendar.timegm((2026, 6, 1, 23, 24, 26, 0, 0, 0))
    assert rec[7] == expected_utc
