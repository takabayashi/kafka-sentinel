import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from config import load_kafka_config, get_kafka_rest_api_credentials, get_kafka_credentials, get_poll_interval
from consumer_group_api import ConsumerGroupAPIClient
from metrics_storage import MetricsWindow
from metrics_publisher import MetricsPublisher

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VelocityMonitor:
    def __init__(self):
        kafka_config = load_kafka_config()
        kafka_rest_api_key, kafka_rest_api_secret = get_kafka_rest_api_credentials()
        kafka_api_key, kafka_api_secret = get_kafka_credentials()

        self.cluster_id = kafka_config["cluster_id"]
        self.poll_interval = get_poll_interval()

        self.api_client = ConsumerGroupAPIClient(
            rest_endpoint=kafka_config["kafka_rest_endpoint"],
            api_key=kafka_rest_api_key,
            api_secret=kafka_rest_api_secret,
            cluster_id=self.cluster_id
        )

        self.metrics_storage = MetricsWindow(window_size_seconds=86400)

        self.publisher = MetricsPublisher(
            bootstrap_servers=kafka_config["bootstrap_servers"],
            api_key=kafka_api_key,
            api_secret=kafka_api_secret,
            topic=kafka_config["metrics_topic"]
        )

        logger.info(f"Velocity Monitor initialized for cluster {self.cluster_id}, polling every {self.poll_interval}s")

    def run(self):
        logger.info("Starting Velocity Monitor polling loop...")
        while True:
            try:
                self._poll_and_publish()
                time.sleep(self.poll_interval)
            except KeyboardInterrupt:
                logger.info("Shutting down Velocity Monitor...")
                self.publisher.close()
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}", exc_info=True)
                time.sleep(self.poll_interval)

    def _poll_and_publish(self):
        groups = self.api_client.list_consumer_groups()
        if not groups:
            logger.debug("No consumer groups found")
            return

        logger.info(f"Polling {len(groups)} consumer groups...")
        for group_id in groups:
            try:
                metrics = self._compute_group_metrics(group_id)
                if metrics:
                    self.publisher.publish(metrics)
            except Exception as e:
                logger.error(f"Failed to process group {group_id}: {e}")

        self.publisher.flush(timeout=5.0)

    def _compute_group_metrics(self, group_id: str) -> dict:
        lag_data = self.api_client.get_consumer_group_lag(group_id)
        if not lag_data:
            return None

        timestamp = datetime.utcnow()
        total_lag = 0
        total_consumed_offset = 0
        total_log_end_offset = 0
        partition_lags = []
        partition_details = []

        for partition_lag in lag_data:
            lag = partition_lag.get("lag", 0)
            consumed_offset = partition_lag.get("current_offset", 0)
            log_end_offset = partition_lag.get("log_end_offset", 0)

            total_lag += lag
            total_consumed_offset += consumed_offset
            total_log_end_offset += log_end_offset
            partition_lags.append(lag)

            partition_details.append({
                "partition_id": partition_lag.get("partition_id"),
                "topic": partition_lag.get("topic_name"),
                "lag": lag,
                "consumed_offset": consumed_offset,
                "log_end_offset": log_end_offset,
            })

        topic_name = lag_data[0].get("topic_name", "unknown") if lag_data else "unknown"

        read_speed, write_speed = self.metrics_storage.compute_speeds(group_id, total_consumed_offset, total_log_end_offset, timestamp)
        self.metrics_storage.record(group_id, total_lag, total_consumed_offset, total_log_end_offset, timestamp)

        lag_24h_avg = self.metrics_storage.get_lag_average_24h(group_id)
        lag_percentile_95 = self.metrics_storage.get_lag_percentile_95(group_id)

        partition_skew_score = self._compute_partition_skew(partition_lags)
        time_to_catchup = total_lag / read_speed if read_speed > 0 else 0

        metrics = {
            "timestamp": timestamp.isoformat() + "Z",
            "cluster_id": self.cluster_id,
            "consumer_group": group_id,
            "topic": topic_name,
            "current_lag": total_lag,
            "read_speed": round(read_speed, 2),
            "write_speed": round(write_speed, 2),
            "time_to_catchup_seconds": round(time_to_catchup, 2),
            "partition_skew_score": round(partition_skew_score, 2),
            "lag_24h_avg": round(lag_24h_avg, 2),
            "lag_percentile_95_24h": round(lag_percentile_95, 2),
            "partition_count": len(partition_details),
            "partitions": partition_details
        }

        logger.debug(f"Group {group_id}: lag={total_lag}, read_speed={read_speed:.2f} msg/s, write_speed={write_speed:.2f} msg/s, skew={partition_skew_score:.2f}")
        return metrics

    @staticmethod
    def _compute_partition_skew(partition_lags: list) -> float:
        if not partition_lags or len(partition_lags) == 1:
            return 0.0
        max_lag = max(partition_lags)
        avg_lag = sum(partition_lags) / len(partition_lags)
        if avg_lag == 0:
            return 0.0
        skew = max_lag / avg_lag
        return min(skew, 5.0)

if __name__ == '__main__':
    monitor = VelocityMonitor()
    monitor.run()
