import time
import threading
import logging
from typing import Optional
from kafka_producer import KafkaProducerWrapper
from event_schema import create_event
from config_loader import load_kafka_config

logger = logging.getLogger(__name__)


class FreeProducer:
    """
    Continuous event generator with configurable throughput.
    Used to establish baseline metrics for ARIMA training.
    """

    def __init__(self, producer: KafkaProducerWrapper):
        self.producer = producer
        self.kafka_config = load_kafka_config()
        self.topic = self.kafka_config["simulator_topic"]

        self._running = False
        self._thread: Optional[threading.Thread] = None

        # Configuration (can be updated via API)
        self.throughput = 100  # messages per second
        self.consumer_group = "checkout-service"
        self.target_topic = "orders"

    def start(
        self,
        throughput: int = 100,
        consumer_group: str = "checkout-service",
        target_topic: str = "orders"
    ):
        """
        Start continuous event generation.

        Args:
            throughput: Messages per second
            consumer_group: Simulated consumer group name
            target_topic: Target Kafka topic for simulated data
        """
        if self._running:
            logger.warning("Free producer already running")
            return

        self.throughput = throughput
        self.consumer_group = consumer_group
        self.target_topic = target_topic

        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

        logger.info(
            f"Free producer started: {throughput} msg/s to {consumer_group}/{target_topic}"
        )

    def stop(self):
        """Stop the free producer"""
        if not self._running:
            return

        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)

        logger.info("Free producer stopped")

    def is_running(self) -> bool:
        """Check if free producer is running"""
        return self._running

    def _run(self):
        """Main producer loop"""
        interval = 1.0 / self.throughput  # seconds between messages
        message_count = 0

        while self._running:
            try:
                event = create_event(
                    consumer_group=self.consumer_group,
                    topic=self.target_topic,
                    message_count=1,
                    sequence_number=message_count
                )

                self.producer.produce(
                    topic=self.topic,
                    value=event,
                    key=self.consumer_group
                )

                message_count += 1
                time.sleep(interval)

            except Exception as e:
                logger.error(f"Error in free producer: {e}")
                time.sleep(1.0)  # Back off on error
