from confluent_kafka import Producer
from typing import Optional, Callable
import logging

logger = logging.getLogger(__name__)


class KafkaProducerWrapper:
    """Wrapper for confluent-kafka Producer with retry logic and callbacks"""

    def __init__(self, config: dict):
        self.config = config
        self.producer = Producer(config)

    def produce(
        self,
        topic: str,
        value: str,
        key: Optional[str] = None,
        partition: Optional[int] = None,
        on_delivery: Optional[Callable] = None
    ):
        """
        Produce a message to Kafka topic.

        Args:
            topic: Kafka topic name
            value: Message value (JSON string)
            key: Optional message key
            partition: Optional specific partition
            on_delivery: Optional delivery callback
        """
        # Build producer kwargs
        produce_kwargs = {
            "topic": topic,
            "value": value.encode('utf-8'),
            "on_delivery": on_delivery or self._default_delivery_callback
        }

        if key is not None:
            produce_kwargs["key"] = key.encode('utf-8')

        if partition is not None:
            produce_kwargs["partition"] = partition

        try:
            self.producer.produce(**produce_kwargs)
            # Poll to handle delivery callbacks
            self.producer.poll(0)
        except BufferError:
            logger.warning(f"Local producer queue full, flushing...")
            self.producer.flush()
            self.producer.produce(**produce_kwargs)

    def flush(self, timeout: float = 10.0):
        """Flush all pending messages"""
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered")

    def close(self):
        """Close the producer and flush remaining messages"""
        self.flush()

    @staticmethod
    def _default_delivery_callback(err, msg):
        """Default callback for message delivery reports"""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} "
                f"[partition {msg.partition()}] at offset {msg.offset()}"
            )
