import logging
import json
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
    Base class for all Kafka Producers.
    Handles: Schema Registry connection, Avro serialization,
             delivery callbacks, DLQ routing on error.
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

        # Schema Registry
        self.schema_registry_client = SchemaRegistryClient(
            {"url": schema_registry_url}
        )

        # Avro Serializer
        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            schema_str,
            self.to_dict  # converts object to dict before serialization
        )

        # Kafka Producer
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            # Exactly-once settings
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "retry.backoff.ms": 1000,
            # Performance settings
            "linger.ms": 5,  # wait 5ms to batch messages
            "batch.size": 65536,  # 64KB batch size
            "compression.type": "lz4",
        })

        self.logger.info(
            f"Producer initialized | topic={topic} | "
            f"broker={bootstrap_servers}"
        )

    def delivery_callback(self, err, msg):
        """
        Called by Kafka after each message is delivered or fails.
        On error: routes message to DLQ topic.
        """
        if err:
            self.logger.error(
                f"Delivery failed | topic={msg.topic()} | "
                f"partition={msg.partition()} | error={err}"
            )
            self._send_to_dlq(msg)
        else:
            self.logger.debug(
                f"Delivered | topic={msg.topic()} | "
                f"partition={msg.partition()} | "
                f"offset={msg.offset()}"
            )

    def _send_to_dlq(self, failed_msg):
        """
        Routes failed messages to Dead Letter Queue topic.
        Preserves original message value and adds error metadata.
        """
        dlq_payload = json.dumps({
            "original_topic": failed_msg.topic(),
            "original_key": failed_msg.key(),
            "original_value": failed_msg.value().decode("utf-8")
            if failed_msg.value() else None,
            "error": "delivery_failed"
        }).encode("utf-8")

        self.producer.produce(
            topic="dlq-events",
            value=dlq_payload,
            key=failed_msg.key()
        )
        self.logger.warning(
            f"Message routed to DLQ | "
            f"original_topic={failed_msg.topic()}"
        )

    def send(self, key: str, value: object):
        """
        Serialize and send a message to Kafka.
        key = symbol (BTCUSDT) determines partition
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
            # Non-blocking: polls for delivery callbacks
            self.producer.poll(0)

        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            self._send_error_to_dlq(key, value, str(e))

    def _send_error_to_dlq(self, key: str, value: object, error: str):
        """
        Routes serialization errors to DLQ.
        """
        dlq_payload = json.dumps({
            "original_topic": self.topic,
            "original_key": key,
            "error": error,
            "raw_value": str(value)
        }).encode("utf-8")

        self.producer.produce(
            topic="dlq-events",
            value=dlq_payload,
            key=key.encode("utf-8")
        )
        self.logger.warning(f"Serialization error routed to DLQ | key={key}")

    def flush(self):
        """
        Wait for all pending messages to be delivered.
        Call before shutdown.
        """
        pending = self.producer.flush(timeout=30)
        if pending > 0:
            self.logger.warning(f"{pending} messages not delivered on flush")
        else:
            self.logger.info("All messages delivered successfully")

    @abstractmethod
    def to_dict(self, obj, ctx) -> dict:
        """
        Convert domain object to dict for Avro serialization.
        Must be implemented by each Producer.
        """
        pass

    @abstractmethod
    async def start(self):
        """
        Start WebSocket connection and begin producing.
        Must be implemented by each Producer.
        """
        pass
