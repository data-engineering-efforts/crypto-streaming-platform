-- VWAP aggregations
CREATE TABLE IF NOT EXISTS vwap_aggregations
(
    symbol String,
    window_start DateTime,
    window_end DateTime,
    vwap Float64,
    total_volume Float64,
    trade_count UInt64,
    interval_label String,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, window_start, interval_label);

-- Whale alerts
CREATE TABLE IF NOT EXISTS whale_alerts
(
    symbol String,
    price Float64,
    quantity Float64,
    trade_value Float64,
    exchange String,
    side String,
    event_time DateTime,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, event_time);

-- Arbitrage signals
CREATE TABLE IF NOT EXISTS arbitrage_signals
(
    symbol String,
    binance_price Float64,
    coinbase_price Float64,
    spread Float64,
    spread_pct Float64,
    direction String,
    event_time DateTime,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, event_time);

-- Double Bottom CEP signals
CREATE TABLE IF NOT EXISTS double_bottom_signals
(
    symbol String,
    first_dip Float64,
    second_dip Float64,
    peak_price Float64,
    breakout_price Float64,
    confirmed_at DateTime,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, confirmed_at);

-- Reconciliation results
CREATE TABLE IF NOT EXISTS recon_results
(
    symbol String,
    window_start DateTime,
    window_end DateTime,
    streaming_vwap Float64,
    batch_vwap Float64,
    delta Float64,
    delta_pct Float64,
    status String,
    checked_at DateTime,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, window_start);
