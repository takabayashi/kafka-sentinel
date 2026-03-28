from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer
from pathlib import Path
import logging
import os
import json as json_lib

logger = logging.getLogger(__name__)

class MetricsPublisher:
    def __init__(self, bootstrap_servers: str, api_key: str, api_secret: str, topic: str):
        self.topic = topic

        # Get Schema Registry credentials from environment
        sr_api_key = os.getenv("SCHEMA_REGISTRY_API_KEY")
        sr_api_secret = os.getenv("SCHEMA_REGISTRY_API_SECRET")
        sr_url = os.getenv("SCHEMA_REGISTRY_URL", "https://psrc-817x7r.us-west2.gcp.confluent.cloud")

        if not sr_api_key or not sr_api_secret:
            raise ValueError("SCHEMA_REGISTRY_API_KEY and SCHEMA_REGISTRY_API_SECRET must be set in .env")

        # Load JSON schema for metrics_flattened
        schema_path = Path(__file__).parent / "metrics_flattened_schema.json"
        with open(schema_path) as f:
            schema_str = f.read()

        # Configure Schema Registry client
        schema_registry_conf = {
            'url': sr_url,
            'basic.auth.user.info': f'{sr_api_key}:{sr_api_secret}'
        }
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # Create JSON serializer for the value
        json_serializer = JSONSerializer(
            schema_str=schema_str,
            schema_registry_client=schema_registry_client
        )

        # Configure producer with serializers
        producer_conf = {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": api_key,
            "sasl.password": api_secret,
            "client.id": "velocity-monitor",
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': json_serializer
        }
        self.producer = SerializingProducer(producer_conf)

    def publish(self, metrics: dict):
        try:
            consumer_group = metrics.get("consumer_group", "unknown")
            self.producer.produce(
                topic=self.topic,
                key=consumer_group,
                value=metrics,
                on_delivery=self._delivery_callback
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to publish metrics for {metrics.get('consumer_group')}: {e}", exc_info=True)

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
