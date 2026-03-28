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
        "kafka_rest_endpoint": config["kafka_rest_endpoint"]["value"],
        "cluster_id": config["cluster_id"]["value"],
        "environment_id": config["environment_id"]["value"],
        "metrics_topic": config["topic_names"]["value"]["metrics_flattened"],
    }


def get_kafka_rest_api_credentials() -> tuple[str, str]:
    """
    Get Kafka API credentials for REST API v3 calls.

    NOTE: Confluent Cloud REST API v3 uses the SAME credentials as Kafka (cluster API keys),
    NOT separate Cloud API keys.
    """
    api_key = os.getenv("KAFKA_API_KEY")
    api_secret = os.getenv("KAFKA_API_SECRET")

    if not api_key or not api_secret:
        raise ValueError(
            "KAFKA_API_KEY and KAFKA_API_SECRET must be set. "
            "REST API v3 uses the same Kafka cluster credentials."
        )

    return api_key, api_secret


def get_kafka_credentials() -> tuple[str, str]:
    """Get Kafka API credentials for producing metrics"""
    api_key = os.getenv("KAFKA_API_KEY")
    api_secret = os.getenv("KAFKA_API_SECRET")

    if not api_key or not api_secret:
        raise ValueError(
            "KAFKA_API_KEY and KAFKA_API_SECRET must be set. "
            "These are for publishing metrics to Kafka."
        )

    return api_key, api_secret


def get_poll_interval() -> int:
    """Get polling interval in seconds"""
    return int(os.getenv("POLL_INTERVAL_SECONDS", "10"))
