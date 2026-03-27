import time
import sys
sys.path.append('..')
from scenarios.base_scenario import BaseScenario
from event_schema import create_event


class ConsumerSlowScenario(BaseScenario):
    """
    Consumer Slow: Simulates degraded consumer performance.

    Publishes events at normal rate but marks them as "slow consumer" pattern,
    indicating consumers are processing at 10% of normal speed. Triggers speed
    trending down anomaly detection.
    """

    @property
    def name(self) -> str:
        return "consumer_slow"

    @property
    def description(self) -> str:
        return "Reduces consumption rate to 10% while producer maintains normal pace"

    @property
    def duration_seconds(self) -> int:
        return 120

    def _execute(self, consumer_group: str, target_topic: str, **kwargs):
        """
        Signal consumer slowdown pattern.

        Pattern:
        - Publish at normal rate (100 msg/s)
        - Tag events with "consumer_slow" indicator
        - Duration: 120 seconds for clear trend detection
        """
        normal_rate = kwargs.get("producer_rate", 100)  # msg/s
        interval = 1.0 / normal_rate

        end_time = time.time() + self.duration_seconds
        message_count = 0

        while time.time() < end_time:
            event = create_event(
                consumer_group=consumer_group,
                topic=target_topic,
                scenario_name=self.name,
                scenario_id=self.scenario_id,
                message_count=1,
                consumer_speed_degradation=0.1,  # 10% of normal speed
                sequence_number=message_count
            )

            self.producer.produce(
                topic=self.topic,
                value=event,
                key=consumer_group
            )

            message_count += 1
            time.sleep(interval)
