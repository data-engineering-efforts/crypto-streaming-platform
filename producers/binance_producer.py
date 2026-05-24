import asyncio
import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal

import os
from pathlib import Path
from dotenv import load_dotenv

import websockets
from confluent_kafka.schema_registry.avro import AvroSerializer

from base_producer import BaseProducer

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)

kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
schema_registry = os.environ.get("SCHEMA_REGISTRY_URL")

# WebSocket URLs
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"

# Symbols we track
# 3 symbols = 3 Kafka partitions (partition key = symbol)
SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]

@dataclass
class BinanceTrade:
    """
    Represents a single trade event from Binance WebSocket.
    Field names map directly to our Avro schema.
    """
    event_type:       str
    event_time:       int    # milliseconds
    symbol:           str
    trade_id:         int
    price:            Decimal
    quantity:         Decimal
    buyer_order_id:   int
    seller_order_id:  int
    trade_time:       int    # milliseconds
    is_buyer_maker:   bool
    ingestion_time:   int    # milliseconds, internal field for latency calculation

class BinanceProducer(BaseProducer):
    """
    Connects to Binance WebSocket combined stream.
    Reads trade events for BTC, ETH, SOL.
    Serializes to Avro and produces to raw-binance-trades topic.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
    ):
        # Load Avro schema from file
        with open("schemas/binance_trade.avsc", "r") as f:
            schema_str = f.read()

        super().__init__(
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            topic="raw-binance-trades",
            schema_str=schema_str,
        )

        # Build combined stream URL
        # Example: btcusdt@trade/ethusdt@trade/solusdt@trade
        streams = "/".join([f"{s}@trade" for s in SYMBOLS])
        self.ws_url = f"{BINANCE_WS_BASE}?streams={streams}"

        self.logger.info(
            f"BinanceProducer ready | "
            f"symbols={SYMBOLS} | "
            f"ws_url={self.ws_url}"
        )

    def to_dict(self, trade: BinanceTrade, ctx) -> dict:
        """
        Maps BinanceTrade dataclass → dict for Avro serialization.
        Converts Decimal to bytes for decimal logical type.
        """
        return {
            "event_type":      trade.event_type,
            "event_time":      trade.event_time,
            "symbol":          trade.symbol,
            "trade_id":        trade.trade_id,
            "price":           trade.price,
            "quantity":        trade.quantity,
            "buyer_order_id":  trade.buyer_order_id,
            "seller_order_id": trade.seller_order_id,
            "trade_time":      trade.trade_time,
            "is_buyer_maker":  trade.is_buyer_maker,
            "ingestion_time":  trade.ingestion_time,
        }

    def _parse_trade(self, raw: dict) -> BinanceTrade:
        """
        Parses raw Binance WebSocket message into BinanceTrade.

        Binance uses short field names to save bandwidth:
          e - event_type
          E - event_time
          s - symbol
          t - trade_id
          p - price
          q - quantity
          b - buyer_order_id (deprecated by Binance, may be absent)
          a - seller_order_id (deprecated by Binance, may be absent)
          T - trade_time
          m - is_buyer_maker
        """
        return BinanceTrade(
            event_type=      raw["e"],
            event_time=      raw["E"],
            symbol=          raw["s"],
            trade_id=        raw["t"],
            price=           Decimal(raw["p"]),
            quantity=        Decimal(raw["q"]),
            buyer_order_id=  raw.get("b", 0),
            seller_order_id= raw.get("a", 0),
            trade_time=      raw["T"],
            is_buyer_maker=  raw["m"],
            ingestion_time=  int(time.time() * 1000),  # now in ms
        )

    def _calculate_latency(self, trade: BinanceTrade) -> int:
        """
        Calculates pipeline latency in milliseconds.
        latency = ingestion_time - trade_time
        Logged for observability — shown on Grafana dashboard.
        """
        return trade.ingestion_time - trade.trade_time

    async def _handle_message(self, raw_message: str):
        """
        Processes a single WebSocket message.

        Binance combined stream wraps each event:
        {
          "stream": "btcusdt@trade",
          "data": { ...trade event... }
        }
        """
        try:
            message = json.loads(raw_message)

            # Combined stream wraps data in "data" field
            trade_data = message.get("data", message)

            # Skip non-trade events
            if trade_data.get("e") != "trade":
                return

            trade = self._parse_trade(trade_data)

            latency_ms = self._calculate_latency(trade)
            # change log level to DEBUG for high volume of trades, INFO for monitoring
            self.logger.debug(
                f"Trade received | symbol={trade.symbol} | "
                f"price={trade.price} | qty={trade.quantity} | "
                f"latency={latency_ms}ms"
            )

            # Send to Kafka
            # Key = symbol → determines partition
            # BTCUSDT → partition 0
            # ETHUSDT → partition 1
            # SOLUSDT → partition 2
            self.send(
                key=trade.symbol,
                value=trade
            )

            # Log whale trades for monitoring
            if trade.quantity >= Decimal("50"):
                self.logger.warning(
                    f"🐋 WHALE TRADE | symbol={trade.symbol} | "
                    f"qty={trade.quantity} BTC | "
                    f"value=${float(trade.price * trade.quantity):,.0f} | "
                    f"is_buyer_maker={trade.is_buyer_maker}"
                )

        except Exception as e:
            self.logger.error(f"Failed to handle message: {e} | raw={raw_message}")

    async def start(self):
        """
        Connects to Binance WebSocket and starts consuming.
        Implements exponential backoff reconnection strategy.
        """
        reconnect_delay = 1   # seconds
        max_delay = 60        # max backoff

        self.logger.info("Starting BinanceProducer...")

        while True:
            try:
                self.logger.info(f"Connecting to Binance WS: {self.ws_url}")

                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,    # send ping every 20s
                    ping_timeout=10,     # wait 10s for pong
                    close_timeout=10,
                ) as websocket:

                    self.logger.info(
                        f"Connected to Binance WebSocket | "
                        f"symbols={SYMBOLS}"
                    )
                    reconnect_delay = 1  # reset backoff on success

                    async for raw_message in websocket:
                        await self._handle_message(raw_message)

            except websockets.exceptions.ConnectionClosedError as e:
                self.logger.warning(
                    f"WebSocket connection closed: {e} | "
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
            finally:
                # Flush pending messages before reconnect
                self.flush()

            # Exponential backoff
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)

async def main():
    producer = BinanceProducer(
        bootstrap_servers=kafka_servers,
        schema_registry_url=schema_registry,
    )
    try:
        await producer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down BinanceProducer...")
        producer.flush()


if __name__ == "__main__":
    asyncio.run(main())
