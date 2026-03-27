import time
import sys
sys.path.append('..')
from scenarios.base_scenario import BaseScenario
from event_schema import create_event


class HotPartitionScenario(BaseScenario):
    """
    Hot Partition: Creates uneven partition load (partition skew).

    Routes 80% of messages to a single partition while distributing 20% across
    others. This simulates poor key distribution or a single high-volume producer
    using the same key. Triggers partition skew anomaly detection.
    """

    @property
    def name(self) -> str:
        return "hot_partition"

    @property
    def description(self) -> str:
        return "Routes 80% of messages to single partition, creating partition skew"

    @property
    def duration_seconds(self) -> int:
        return 90

    def _execute(self, consumer_group: str, target_topic: str, **kwargs):
        """
        Inject hot partition pattern.

        Pattern:
        - 80% of messages → partition 0 (hot partition)
        - 20% of messages → distributed across partitions 1-2
        - Rate: 200 msg/s total
        - Duration: 90 seconds
        """
        total_rate = kwargs.get("total_rate", 200)  # msg/s
        hot_partition_ratio = kwargs.get("hot_ratio", 0.8)
        num_partitions = 3  # simulator_events has 3 partitions

        interval = 1.0 / total_rate
        end_time = time.time() + self.duration_seconds
        message_count = 0

        while time.time() < end_time:
            # Determine partition based on ratio
            if (message_count % 10) < (hot_partition_ratio * 10):
                # Route to hot partition (partition 0)
                partition = 0
                partition_type = "hot"
            else:
                # Distribute across other partitions
                partition = 1 + (message_count % 2)
                partition_type = "normal"

            event = create_event(
                consumer_group=consumer_group,
                topic=target_topic,
                scenario_name=self.name,
                scenario_id=self.scenario_id,
                message_count=1,
                target_partition=partition,
                partition_type=partition_type,
                sequence_number=message_count
            )

            self.producer.produce(
                topic=self.topic,
                value=event,
                key=f"{consumer_group}_p{partition}",  # Key influences partition
                partition=partition
            )

            message_count += 1
            time.sleep(interval)
