#!/usr/bin/env python3
"""
Publish one test message to velocity_anomaly_alerts to register schema
"""
import os
from datetime import datetime
from dotenv import load_dotenv
from config import load_kafka_config
from metrics_publisher import MetricsPublisher

load_dotenv()

# Get config
kafka_config = load_kafka_config()
kafka_api_key = os.getenv("KAFKA_API_KEY")
kafka_api_secret = os.getenv("KAFKA_API_SECRET")

# Initialize publisher for velocity_anomaly_alerts
# Create minimal schema matching the Flink table
alerts_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "VelocityAnomalyAlert",
    "type": "object",
    "properties": {
        "alert_id": {"type": "string"},
        "alert_time": {"type": "string", "format": "date-time"},
        "detection_type": {"type": "string"},
        "severity": {"type": "string"},
        "cluster_id": {"type": "string"},
        "consumer_group": {"type": "string"},
        "topic": {"type": "string"},
        "current_lag": {"type": "integer"},
        "read_speed": {"type": "number"},
        "write_speed": {"type": "number"},
        "time_to_catchup_seconds": {"type": "number"},
        "partition_skew_score": {"type": "number"},
        "anomaly_score": {"type": "number"},
        "context": {"type": "string"}
    }
}

# Create publisher with custom schema
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

schema_registry_conf = {
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

json_serializer = JSONSerializer(
    schema_str=str(alerts_schema).replace("'", '"'),
    schema_registry_client=schema_registry_client,
    conf={'auto.register.schemas': True}
)

producer_conf = {
    'bootstrap.servers': kafka_config['bootstrap_servers'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': kafka_api_key,
    'sasl.password': kafka_api_secret,
    'value.serializer': json_serializer
}

producer = SerializingProducer(producer_conf)

# Create minimal test message
test_alert = {
    "alert_id": "bootstrap_test",
    "alert_time": datetime.utcnow().isoformat() + "Z",
    "detection_type": "test",
    "severity": "info",
    "cluster_id": "test-cluster",
    "consumer_group": "test-group",
    "topic": "test-topic",
    "current_lag": 0,
    "read_speed": 0.0,
    "write_speed": 0.0,
    "time_to_catchup_seconds": 0.0,
    "partition_skew_score": 0.0,
    "anomaly_score": 0.0,
    "context": "{}"
}

print("Publishing test alert to register schema...")
producer.produce(
    topic="velocity_anomaly_alerts",
    key="bootstrap_test",
    value=test_alert
)
producer.flush(timeout=10.0)
print("✅ Schema registered in Schema Registry for velocity_anomaly_alerts-value")
