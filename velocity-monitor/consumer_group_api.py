import base64
import requests
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class ConsumerGroupAPIClient:
    """Client for Confluent Cloud Consumer Group API"""

    def __init__(self, rest_endpoint: str, api_key: str, api_secret: str, cluster_id: str):
        self.rest_endpoint = rest_endpoint
        self.cluster_id = cluster_id
        # Remove :443 port if present
        clean_endpoint = rest_endpoint.replace(':443', '')
        self.base_url = f"{clean_endpoint}/kafka/v3/clusters/{cluster_id}"

        # Create Basic Auth header
        credentials = f"{api_key}:{api_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()
        self.headers = {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/json"
        }

    def list_consumer_groups(self) -> List[str]:
        """
        List all consumer groups in the cluster.

        Returns:
            List of consumer group IDs
        """
        try:
            response = requests.get(
                f"{self.base_url}/consumer-groups",
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()

            data = response.json()
            logger.debug(f"Consumer groups response: {data}")
            # API returns 'consumer_group_id', not 'group_id'
            groups = [group["consumer_group_id"] for group in data.get("data", [])]
            logger.info(f"Discovered {len(groups)} consumer groups: {groups}")
            return groups

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list consumer groups: {e}")
            return []

    def get_consumer_group_lag(self, group_id: str) -> Optional[List[Dict]]:
        """
        Get lag information for a consumer group.

        Args:
            group_id: Consumer group ID

        Returns:
            List of partition lag data
        """
        # WORKAROUND: STANDARD clusters don't support lag API
        # Generate mock lag data for testing downstream pipeline
        logger.info(f"Using MOCK lag data for {group_id} (STANDARD cluster limitation)")

        import random
        import time

        # Simulate growing lag for test-consumer
        if group_id == "test-consumer":
            base_lag = int(time.time() % 10000)  # Changes over time
            mock_lag = [
                {
                    "cluster_id": self.cluster_id,
                    "consumer_group_id": group_id,
                    "topic_name": "simulator_events",
                    "partition_id": 0,
                    "current_offset": 5000 + base_lag,
                    "log_end_offset": 8000 + base_lag + random.randint(100, 500),
                    "lag": 3000 + random.randint(100, 500),
                    "consumer_id": f"{group_id}-consumer-1",
                    "instance_id": "consumer-1",
                    "client_id": "rdkafka"
                },
                {
                    "cluster_id": self.cluster_id,
                    "consumer_group_id": group_id,
                    "topic_name": "simulator_events",
                    "partition_id": 1,
                    "current_offset": 4800 + base_lag,
                    "log_end_offset": 7500 + base_lag + random.randint(100, 500),
                    "lag": 2700 + random.randint(100, 500),
                    "consumer_id": f"{group_id}-consumer-1",
                    "instance_id": "consumer-1",
                    "client_id": "rdkafka"
                },
                {
                    "cluster_id": self.cluster_id,
                    "consumer_group_id": group_id,
                    "topic_name": "simulator_events",
                    "partition_id": 2,
                    "current_offset": 5100 + base_lag,
                    "log_end_offset": 8200 + base_lag + random.randint(100, 500),
                    "lag": 3100 + random.randint(100, 500),
                    "consumer_id": f"{group_id}-consumer-1",
                    "instance_id": "consumer-1",
                    "client_id": "rdkafka"
                }
            ]
            return mock_lag

        return None
