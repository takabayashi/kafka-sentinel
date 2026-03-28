#!/usr/bin/env python3
"""
Publish one test message to metrics_flattened to register schema
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

# Initialize publisher (loads metrics_flattened_schema.json)
publisher = MetricsPublisher(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    api_key=kafka_api_key,
    api_secret=kafka_api_secret,
    topic="metrics_flattened"
)

# Create minimal test message
test_message = {
    "event_time": datetime.utcnow().isoformat() + "Z",
    "cluster_id": "test-cluster",
    "consumer_group": "test-group",
    "topic": "test-topic",
    "current_lag": 0,
    "read_speed": 0.0,
    "write_speed": 0.0,
    "time_to_catchup_seconds": 0.0,
    "partition_skew_score": 0.0,
    "lag_24h_avg": 0.0,
    "lag_percentile_95_24h": 0.0,
    "partition_count": 1,
    "lag_velocity": 0.0,
    "speed_ratio": 1.0,
    "is_falling_behind": False
}

print("Publishing test message to register schema...")
publisher.publish(test_message)
publisher.flush(timeout=10.0)
print("✅ Schema registered in Schema Registry for metrics_flattened-value")
