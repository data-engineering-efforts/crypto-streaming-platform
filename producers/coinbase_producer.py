import asyncio
import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime

import websockets

from base_producer import BaseProducer

logger = logging.getLogger(__name__)

COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"

WHALE_THRESHOLDS = {
    "BTCUSDT": Decimal("5"),
    "ETHUSDT": Decimal("50"),
    "SOLUSDT": Decimal("500"),
}


@dataclass
class CoinbaseTrade:
    """
    Represents a single trade event from Coinbase WebSocket.
    Field names map directly to our Avro schema.
    """
    trade_id: str
    product_id: str  # e.g. "BTC-USD" as returned by Coinbase
    price: Decimal
    size: Decimal
    side: str  # "BUY" or "SELL"
    event_time: int  # Unix timestamp milliseconds
    ingestion_time: int # internal field for latency calculation


class CoinbaseProducer(BaseProducer):
    """
    Connects to Coinbase Advanced Trade WebSocket.
    Reads trade events for BTC, ETH, SOL.
    Serializes to Avro and produces to raw-coinbase-match topic.

    Delivery guarantees:
        1. At-least-once from WebSocket perspective
        2. Idempotent producer prevents network-level duplicates
        3. True exactly-once starts at Flink checkpointing stage
    """

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
    ):
        with open("schemas/coinbase_match.avsc", "r") as f:
            schema_str = f.read()

        super().__init__(
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            topic="raw-coinbase-match",
            schema_str=schema_str,
        )

        self.ws_url = COINBASE_WS_URL
        self.logger.info("CoinbaseProducer ready")

    def to_dict(self, trade: CoinbaseTrade, ctx) -> dict:
        """
        Maps CoinbaseTrade dataclass - dict for Avro serialization.
        Called by AvroSerializer before encoding to binary.
        """
        return {
            "trade_id": trade.trade_id,
            "product_id": trade.product_id,
            "price": trade.price,
            "size": trade.size,
            "side": trade.side,
            "event_time": trade.event_time,
            "ingestion_time": trade.ingestion_time,
        }

    def _normalize_symbol(self, product_id: str) -> str:
        return product_id.replace("-USD", "USDT")

    def _iso_to_ms(self, iso_string: str) -> int:
        dt = datetime.fromisoformat(
            iso_string.replace("Z", "+00:00")
        )
        return int(dt.timestamp() * 1000)

    def _parse_trade(self, raw: dict) -> CoinbaseTrade:
        """
        Parses raw Coinbase trade dict into CoinbaseTrade.

        Coinbase trade fields:
          trade_id - unique trade ID (string)
          product_id - "BTC-USD", "ETH-USD", "SOL-USD"
          price - price string
          size - quantity string
          side - "BUY" or "SELL"
          time - ISO 8601 timestamp string
        """
        return CoinbaseTrade(
            trade_id=raw["trade_id"],
            product_id=raw["product_id"],
            price=Decimal(raw["price"]),
            size=Decimal(raw["size"]),
            side=raw["side"],
            event_time=self._iso_to_ms(raw["time"]),
            ingestion_time=int(time.time() * 1000),
        )

    def _check_whale(self, trade: CoinbaseTrade):
        symbol = self._normalize_symbol(trade.product_id)
        threshold = WHALE_THRESHOLDS.get(
            symbol,
            Decimal("999999")
        )
        if trade.size >= threshold:
            self._log_whale(
                symbol=symbol,
                quantity=trade.size,
                price=trade.price,
                side=trade.side
            )

            self._log_whale(self._normalize_symbol(trade.product_id),
                            trade.size, trade.price, trade.side)

    async def _handle_message(self, raw_message: str):
        """
        Processes a single Coinbase WebSocket message.

        Coinbase message format:
        {
          "channel": "market_trades",
          "events": [
            {
              "type": "update",
              "trades": [{...}, {...}]
            }
          ]
        }
        """
        try:
            message = json.loads(raw_message)

            if message.get("channel") != "market_trades":
                return

            events = message.get("events", [])
            for event in events:
                if event.get("type") not in ("update", "snapshot"):
                    continue

                trades = event.get("trades", [])
                for raw_trade in trades:
                    parsed = self._parse_trade(raw_trade)

                    if parsed is None:
                        continue

                    symbol = self._normalize_symbol(parsed.product_id)

                    self.logger.info(
                        f"Trade | symbol={symbol} | "
                        f"price={parsed.price:.2f} | "
                        f"size={parsed.size:.8f} | " 
                        f"latency={self._calculate_latency(parsed.ingestion_time, parsed.event_time)}ms"
                    )

                    self.send(key=symbol, value=parsed)
                    self._check_whale(parsed)

        except Exception as e:
            self.logger.error(
                f"Failed to handle message: {e} | raw={raw_message}"
            )

    async def start(self):
        """
        Main ingestion loop with exponential backoff reconnection.
        Sends subscribe message after each connection.
        """
        # background task for periodic polling which doesn't block the main WebSocket loop
        asyncio.create_task(self._poll_loop())

        reconnect_delay = 1
        max_delay = 60

        subscribe_msg = {
            "type": "subscribe",
            "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD"],
            "channel": "market_trades"
        }

        self.logger.info("Starting CoinbaseProducer...")

        while True:
            try:
                self.logger.info(
                    f"Connecting to Coinbase WS: {self.ws_url}"
                )

                async with websockets.connect(self.ws_url) as websocket:
                    await websocket.send(json.dumps(subscribe_msg))
                    self.logger.info("Subscribed to market_trades")
                    reconnect_delay = 1

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

            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)