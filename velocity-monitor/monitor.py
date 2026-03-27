#!/usr/bin/env python3
"""
Velocity Monitor - Main Entry Point

Polls Confluent Cloud Consumer Group API to collect real-time metrics
and publishes them to the metrics_source Kafka topic.
"""

import logging
import time
import signal
import sys
from datetime import datetime
from typing import Dict, Any
from dotenv import load_dotenv

from config import (
    load_kafka_config,
    get_kafka_rest_api_credentials,
    get_kafka_credentials,
    get_poll_interval,
)
from consumer_group_api import ConsumerGroupAPIClient
from metrics_publisher import MetricsPublisher

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global shutdown_requested
    logger.info("Shutdown signal received, stopping gracefully...")
    shutdown_requested = True


def compute_metrics(group_id: str, lag_data: list) -> Dict[str, Any]:
    """
    Compute derived metrics from raw lag data.

    Args:
        group_id: Consumer group ID
        lag_data: List of partition lag data from API

    Returns:
        Dictionary of computed metrics
    """
    if not lag_data:
        return None

    # Aggregate across all partitions
    total_lag = 0
    total_current_offset = 0
    total_log_end_offset = 0
    partition_count = len(lag_data)

    topic = None

    for partition_info in lag_data:
        lag = partition_info.get("lag", 0)
        current_offset = partition_info.get("current_offset", 0)
        log_end_offset = partition_info.get("log_end_offset", 0)

        total_lag += lag
        total_current_offset += current_offset
        total_log_end_offset += log_end_offset

        # Get topic name (should be same for all partitions in group)
        if not topic:
            topic = partition_info.get("topic_name", "unknown")

    # Compute partition skew (max lag - min lag)
    lags = [p.get("lag", 0) for p in lag_data]
    partition_skew = max(lags) - min(lags) if lags else 0

    metrics = {
        "consumer_group": group_id,
        "topic": topic,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "current_lag": total_lag,
        "partition_count": partition_count,
        "partition_skew": partition_skew,
        "total_current_offset": total_current_offset,
        "total_log_end_offset": total_log_end_offset,
    }

    return metrics


def main():
    """Main monitoring loop"""
    global shutdown_requested

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Load environment variables
    load_dotenv()

    try:
        # Load configuration
        logger.info("Loading configuration...")
        kafka_config = load_kafka_config()
        rest_api_key, rest_api_secret = get_kafka_rest_api_credentials()
        kafka_api_key, kafka_api_secret = get_kafka_credentials()
        poll_interval = get_poll_interval()

        # Initialize clients
        logger.info(f"Connecting to cluster {kafka_config['cluster_id']}...")
        api_client = ConsumerGroupAPIClient(
            rest_endpoint=kafka_config["kafka_rest_endpoint"],
            api_key=rest_api_key,
            api_secret=rest_api_secret,
            cluster_id=kafka_config["cluster_id"],
        )

        publisher = MetricsPublisher(
            bootstrap_servers=kafka_config["bootstrap_servers"],
            api_key=kafka_api_key,
            api_secret=kafka_api_secret,
            topic=kafka_config["metrics_topic"],
        )

        logger.info(f"Velocity Monitor started. Polling every {poll_interval}s...")
        logger.info(f"Publishing to topic: {kafka_config['metrics_topic']}")

        # Main monitoring loop
        while not shutdown_requested:
            try:
                # List all consumer groups
                consumer_groups = api_client.list_consumer_groups()

                if not consumer_groups:
                    logger.warning("No consumer groups found")
                else:
                    logger.info(f"Monitoring {len(consumer_groups)} consumer groups")

                # Poll each consumer group
                for group_id in consumer_groups:
                    if shutdown_requested:
                        break

                    # Get lag data
                    lag_data = api_client.get_consumer_group_lag(group_id)

                    if lag_data:
                        # Compute metrics
                        metrics = compute_metrics(group_id, lag_data)

                        if metrics:
                            # Publish to Kafka
                            publisher.publish(metrics)
                            logger.debug(f"Published metrics for {group_id}: lag={metrics['current_lag']}")

                # Flush published messages
                publisher.flush()

                # Wait for next poll interval
                if not shutdown_requested:
                    time.sleep(poll_interval)

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}", exc_info=True)
                time.sleep(poll_interval)

        # Cleanup
        logger.info("Shutting down...")
        publisher.close()
        logger.info("Velocity Monitor stopped")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
