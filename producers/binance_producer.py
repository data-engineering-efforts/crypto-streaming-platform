import asyncio
import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal

import websockets

from base_producer import BaseProducer

logger = logging.getLogger(__name__)

BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"
SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]

WHALE_THRESHOLDS = {
    "BTCUSDT": Decimal("5"),
    "ETHUSDT": Decimal("50"),
    "SOLUSDT": Decimal("500"),
}

@dataclass
class BinanceTrade:
    """
    Represents a single trade event from Binance WebSocket.
    Field names map directly to our Avro schema.
    """
    event_type: str
    event_time: int
    symbol: str
    trade_id: int
    price: Decimal
    quantity: Decimal
    trade_time: int
    is_buyer_maker: bool
    ingestion_time: int  # internal field for latency calculation


class BinanceProducer(BaseProducer):
    """
    Connects to Binance WebSocket combined stream.
    Reads trade events for BTC, ETH, SOL.
    Serializes to Avro and produces to raw-binance-trades topic.

    Delivery guarantees:
        1. At-least-once from WebSocket perspective
        (data lost during downtime cannot be replayed)
        2. Idempotent producer prevents network-level duplicates
        3. True exactly-once starts at Flink checkpointing stage
    """

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
    ):
        with open("schemas/binance_trade.avsc", "r") as f:
            schema_str = f.read()

        super().__init__(
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            topic="raw-binance-trades",
            schema_str=schema_str,
        )

        streams = "/".join([f"{s}@trade" for s in SYMBOLS])
        self.ws_url = f"{BINANCE_WS_BASE}?streams={streams}"

        self.logger.info(
            f"BinanceProducer ready | symbols={SYMBOLS}"
        )

    def to_dict(self, trade: BinanceTrade, ctx) -> dict:
        """
        Maps BinanceTrade dataclass - dict for Avro serialization.
        Called by AvroSerializer before encoding to binary.
        """
        return {
            "event_type": trade.event_type,
            "event_time": trade.event_time,
            "symbol": trade.symbol,
            "trade_id": trade.trade_id,
            "price": trade.price,
            "quantity": trade.quantity,
            "trade_time": trade.trade_time,
            "is_buyer_maker": trade.is_buyer_maker,
            "ingestion_time": trade.ingestion_time,
        }

    def _parse_trade(self, raw: dict) -> BinanceTrade:
        """
        Parses raw Binance WebSocket message into BinanceTrade.

        Binance uses short field names to save bandwidth:
          e - event_type
          E - event_time (ms)
          s - symbol
          t - trade_id
          p - price (string)
          q - quantity (string)
          T - trade_time (ms)
          m - is_buyer_maker
          M - undocumented, always true for valid market trades
        """
        # Skip invalid market trades (M=false is extremely rare)
        if not raw.get("M", True):
            return None

        return BinanceTrade(
            event_type=raw["e"],
            event_time=raw["E"],
            symbol=raw["s"],
            trade_id=raw["t"],
            price=Decimal(raw["p"]),
            quantity=Decimal(raw["q"]),
            trade_time=raw["T"],
            is_buyer_maker=raw["m"],
            ingestion_time=int(time.time() * 1000),
        )

    def _check_whale(self, trade: BinanceTrade):
        threshold = WHALE_THRESHOLDS.get(trade.symbol, Decimal("999999"))
        if trade.quantity >= threshold:
            side = 'SELL' if trade.is_buyer_maker else 'BUY'
            
            self._log_whale(
                symbol=trade.symbol,
                quantity=trade.quantity,
                price=trade.price,
                side=side
            )

    async def _handle_message(self, raw_message: str):
        """
        Processes a single WebSocket message.

        Binance combined stream format:
        {
          "stream": "btcusdt@trade",
          "data": { ...trade fields... }
        }
        """
        try:
            message = json.loads(raw_message)
            trade_data = message.get("data", message)

            if trade_data.get("e") != "trade":
                return

            trade = self._parse_trade(trade_data)

            if trade is None:  # filtered by M=false
                return

            # change debug log to info for normal trade events, keep warning for whales
            self.logger.debug(
                f"Trade | symbol={trade.symbol} | "
                f"price={trade.price:.2f} | "
                f"qty={trade.quantity:.8f} | " 
                f"latency={self._calculate_latency(trade.ingestion_time, trade.trade_time)}ms"
)

            # Partition key = symbol (uppercase, as returned by Binance)
            # Kafka hashes key to determine partition:
            # hash("BTCUSDT") % 3, hash("ETHUSDT") % 3, hash("SOLUSDT") % 3
            self.send(key=trade.symbol, value=trade)
            self._check_whale(trade)

        except Exception as e:
            self.logger.error(
                f"Failed to handle message: {e} | raw={raw_message}"
            )

    async def start(self):
        """
        Main ingestion loop with exponential backoff reconnection.
        """
        # _poll_loop() runs as background task for delivery callbacks and do not block the main WebSocket loop
        asyncio.create_task(self._poll_loop())

        reconnect_delay = 1
        max_delay = 60

        self.logger.info("Starting BinanceProducer...")

        while True:
            try:
                self.logger.info(
                    f"Connecting to Binance WS | url={self.ws_url}"
                )

                async with websockets.connect(
                    self.ws_url,
                    ping_interval=None,
                    close_timeout=10,
                ) as websocket:

                    self.logger.info(
                        f"Connected | symbols={SYMBOLS}"
                    )
                    reconnect_delay = 1  # reset on successful connection

                    async for raw_message in websocket:
                        await self._handle_message(raw_message)

            except websockets.exceptions.ConnectionClosedError as e:
                self.logger.warning(
                    f"Connection closed: {e} | "
                    f"reconnecting in {reconnect_delay}s"
                )
            except websockets.exceptions.WebSocketException as e:
                self.logger.error(
                    f"WebSocket error: {e} | "
                    f"reconnecting in {reconnect_delay}s"
                )
            except Exception as e:
                self.logger.error(
                    f"Unexpected error: {e} | "
                    f"reconnecting in {reconnect_delay}s"
                )

            # exponential backoff
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)