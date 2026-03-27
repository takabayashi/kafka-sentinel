import json
import os
from pathlib import Path
from typing import Dict, Any


def load_kafka_config() -> Dict[str, Any]:
    """Load Kafka configuration from kafka-config.json"""
    config_path = Path(__file__).parent.parent / "config" / "kafka-config.json"

    if not config_path.exists():
        raise FileNotFoundError(
            f"Kafka config not found at {config_path}. "
            "Run 'terraform apply' in infra/terraform first."
        )

    with open(config_path) as f:
        config = json.load(f)

    # Extract values from Terraform output format
    return {
        "bootstrap_servers": config["kafka_bootstrap_endpoint"]["value"].replace("SASL_SSL://", ""),
        "simulator_topic": config["topic_names"]["value"]["simulator_events"],
        "metrics_topic": config["topic_names"]["value"]["metrics_source"],
    }


def get_kafka_producer_config() -> Dict[str, str]:
    """Get confluent-kafka producer configuration"""
    kafka_config = load_kafka_config()

    kafka_api_key = os.getenv("KAFKA_API_KEY")
    kafka_api_secret = os.getenv("KAFKA_API_SECRET")

    if not kafka_api_key or not kafka_api_secret:
        raise ValueError(
            "KAFKA_API_KEY and KAFKA_API_SECRET environment variables must be set. "
            "Copy .env.example to .env and fill in your Kafka API credentials."
        )

    return {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": kafka_api_key,
        "sasl.password": kafka_api_secret,
        "client.id": "ifood-anomaly-simulator",
    }
