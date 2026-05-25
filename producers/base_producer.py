import asyncio
import logging
import json
import base64
import time
from decimal import Decimal
from abc import ABC, abstractmethod
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)


class BaseProducer(ABC):
    """
    Base class for all Kafka Producers in the project.
    Handles: Schema Registry connection, Avro serialization,
             delivery callbacks, and centralized DLQ routing.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        schema_str: str,
    ):
        self.topic = topic
        self.logger = logging.getLogger(self.__class__.__name__)

        # Initialize Schema Registry client
        self.schema_registry_client = SchemaRegistryClient(
            {"url": schema_registry_url}
        )

        # Configure Avro Serializer for message values
        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            schema_str,
            self.to_dict
        )

        # Configure Kafka Producer with reliability and batching settings
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            # Idempotence prevents duplicates caused by producer retries (network-level only).
            # True exactly-once begins at Flink checkpointing stage.
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "retry.backoff.ms": 1000,
            # High throughput performance optimization
            "linger.ms": 5,
            "batch.size": 65536,
            "compression.type": "lz4",
        })

        self.logger.info(
            f"Producer initialized | topic={topic} | broker={bootstrap_servers}"
        )

    async def _poll_loop(self):
        self.logger.info("Starting background Kafka poll loop...")
        try:
            while True:
                # Check for completed deliveries (triggers delivery_callback)
                # timeout=0 means non-blocking check
                self.producer.poll(0)
                # Yield control to allow other async tasks (like websocket) to run
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            self.logger.info("Kafka poll loop task cancelled.")

    def delivery_callback(self, err, msg):
        """
        Triggered by poll() once the message is delivered or fails.
        Called by librdkafka with exactly two parameters: (err, msg).
        """
        if err:
            self.logger.error(
                f"Delivery failed | topic={msg.topic()} | error={err}"
            )
            key_str = msg.key().decode("utf-8") if msg.key() else "unknown"
            value_repr = (
                base64.b64encode(msg.value()).decode("utf-8")
                if msg.value() else "empty_payload"
            )
            self._route_to_dlq(
                key=key_str,
                value_representation=value_repr,
                error_message=f"KafkaDeliveryError: {str(err)}"
            )
        else:
            self.logger.debug(
                f"Delivered | topic={msg.topic()} | "
                f"partition={msg.partition()} | offset={msg.offset()}"
            )

    def send(self, key: str, value: object):
        """
        Serializes and pushes message to Kafka internal buffer.
        """
        try:
            serialized_value = self.avro_serializer(
                value,
                SerializationContext(self.topic, MessageField.VALUE)
            )
            self.producer.produce(
                topic=self.topic,
                key=key.encode("utf-8"),
                value=serialized_value,
                on_delivery=self.delivery_callback
            )
        except Exception as e:
            self.logger.error(f"Serialization or produce failed: {e}")
            self._route_to_dlq(
                key=key,
                value_representation=str(value),
                error_message=f"LocalProducerError: {str(e)}"
            )

    def _route_to_dlq(self, key: str, value_representation: str, error_message: str):
        """
        Centralized routing for failed events to Dead Letter Queue.
        """
        try:
            dlq_payload = json.dumps({
                "original_topic": self.topic,
                "original_key": key,
                "payload": value_representation,
                "error": error_message,
                "failed_at": int(time.time() * 1000)
            }).encode("utf-8")

            self.producer.produce(
                topic="dlq-events",
                value=dlq_payload,
                key=key.encode("utf-8") if key else b"unknown"
            )
            self.logger.warning(
                f"Event routed to DLQ | key={key} | reason={error_message}"
            )
        except Exception as dlq_err:
            self.logger.critical(
                f"CRITICAL: DLQ routing failed! Data may be lost. "
                f"Error: {dlq_err} | Original key: {key}"
            )

    def flush(self):
        """
        Blocks until all pending messages are delivered or timeout expires.
        Call before shutdown to prevent data loss.
        """
        self.logger.info("Flushing pending messages...")
        pending = self.producer.flush(timeout=30)
        if pending > 0:
            self.logger.warning(
                f"{pending} messages undelivered after flush timeout"
            )
        else:
            self.logger.info("All messages flushed successfully")

    def _calculate_latency(self, ingestion_time: int, event_time: int) -> int:
        return ingestion_time - event_time

    def _log_whale(self, symbol: str, quantity: Decimal,
                price: Decimal, side: str):
        base_asset = symbol.replace("USDT", "")
        self.logger.warning(
            f"WHALE TRADE | "
            f"symbol={symbol} | "
            f"qty={quantity} {base_asset} | "
            f"value=${float(price * quantity):,.0f} | "
            f"side={side}"
        )

    @abstractmethod
    def to_dict(self, obj, ctx) -> dict:
        """Converts domain object to dict before Avro serialization."""
        pass

    @abstractmethod
    async def start(self):
        """
        Starts the main ingestion loop.
        Must call asyncio.create_task(self._poll_loop()) at the beginning!
        """
        pass