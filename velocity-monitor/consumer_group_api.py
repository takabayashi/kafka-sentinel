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
        try:
            response = requests.get(
                f"{self.base_url}/consumer-groups/{group_id}/lag",
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()

            data = response.json()
            return data.get("data", [])

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.debug(f"Consumer group {group_id} not found or has no lag")
                return None
            logger.error(f"Failed to get lag for group {group_id}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for group {group_id}: {e}")
            return None
