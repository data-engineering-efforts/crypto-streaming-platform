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

SYMBOL_PARTITIONS = {"BTCUSDT": 0, "ETHUSDT": 1, "SOLUSDT": 2}


class BaseProducer(ABC):
    """Base class for all Kafka producers in the project.

    Handles the Schema Registry connection, Avro serialization,
    delivery callbacks, and centralized DLQ routing. Subclasses implement
    `to_dict` and `start`.

    Args:
        bootstrap_servers: Comma-separated Kafka broker addresses.
        schema_registry_url: URL of the Confluent Schema Registry.
        topic: Kafka topic that messages are produced to.
        schema_str: Avro schema (JSON string) for message values.

    Attributes:
        topic: Kafka topic that messages are produced to.
        logger: Logger named after the concrete subclass.
        schema_registry_client: Client for the Schema Registry.
        avro_serializer: Serializer for message values; calls `to_dict`.
        producer: Idempotent confluent-kafka `Producer` (acks=all, lz4).
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
        """Handles the delivery report for one produced message.

        Triggered by `poll()` once the message is delivered or fails.
        Called by librdkafka with exactly two parameters: (err, msg).
        Failed messages are routed to the DLQ.

        Args:
            err: Delivery error, or None if the message was delivered.
            msg: The Kafka message the report refers to.
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
        """Serializes a message and pushes it to the Kafka internal buffer.

        Known symbols are sent to a fixed partition (`SYMBOL_PARTITIONS`).
        Serialization or produce errors are logged and routed to the DLQ
        instead of being raised.

        Args:
            key: Message key (the trading symbol, e.g. "BTCUSDT").
            value: Domain object to serialize via `to_dict` and Avro.
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
                partition=SYMBOL_PARTITIONS.get(key, -1),
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
                f"CRITICAL: DLQ routing failed. Data may be lost. "
                f"Error: {dlq_err} | Original key: {key}"
            )

    def flush(self):
        """Blocks until all pending messages are delivered or 30s pass.

        Call before shutdown to prevent data loss. Logs a warning if
        messages are still undelivered after the timeout.
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
        """Converts a domain object to a dict before Avro serialization.

        Args:
            obj: Domain object (e.g. a trade dataclass) to convert.
            ctx: Serialization context passed by `AvroSerializer`.

        Returns:
            Dict whose keys match the fields of the Avro schema.
        """
        pass

    @abstractmethod
    async def start(self):
        """Starts the main ingestion loop.

        Implementations must call `asyncio.create_task(self._poll_loop())`
        at the beginning so delivery callbacks are processed.
        """
        pass