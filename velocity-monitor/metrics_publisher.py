from confluent_kafka import Producer
import json
import logging

logger = logging.getLogger(__name__)

class MetricsPublisher:
    def __init__(self, bootstrap_servers: str, api_key: str, api_secret: str, topic: str):
        self.topic = topic
        config = {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": api_key,
            "sasl.password": api_secret,
            "client.id": "velocity-monitor",
        }
        self.producer = Producer(config)

    def publish(self, metrics: dict):
        try:
            consumer_group = metrics.get("consumer_group", "unknown")
            value = json.dumps(metrics)
            self.producer.produce(topic=self.topic, key=consumer_group.encode('utf-8'), value=value.encode('utf-8'), on_delivery=self._delivery_callback)
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to publish metrics for {metrics.get('consumer_group')}: {e}")

    def flush(self, timeout: float = 10.0):
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages not delivered during flush")

    def close(self):
        self.flush()

    @staticmethod
    def _delivery_callback(err, msg):
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Metrics published to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")
