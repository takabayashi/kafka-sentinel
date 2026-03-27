import time
import sys
sys.path.append('..')
from scenarios.base_scenario import BaseScenario
from event_schema import create_event


class LagSpikeScenario(BaseScenario):
    """
    Lag Spike: Rapidly increases consumer lag from normal to 45K+ messages.

    Simulates a sudden burst of messages to a topic while consumers maintain
    normal throughput, causing lag to spike. Triggers lag trending anomaly detection.
    """

    @property
    def name(self) -> str:
        return "lag_spike"

    @property
    def description(self) -> str:
        return "Rapidly increases consumer lag from 2K to 45K+ messages"

    @property
    def duration_seconds(self) -> int:
        return 90

    def _execute(self, consumer_group: str, target_topic: str, **kwargs):
        """
        Inject lag spike by rapidly publishing messages.

        Pattern:
        - T+0-10s: Publish 45K messages as fast as possible
        - T+10-90s: Maintain elevated rate (500 msg/s) to keep lag high
        """
        # Phase 1: Rapid burst (45K messages in ~10 seconds)
        burst_size = kwargs.get("burst_size", 45000)
        batch_size = 1000  # Send in batches for better performance

        for i in range(0, burst_size, batch_size):
            event = create_event(
                consumer_group=consumer_group,
                topic=target_topic,
                scenario_name=self.name,
                scenario_id=self.scenario_id,
                message_count=batch_size,
                phase="burst",
                batch_number=i // batch_size
            )

            self.producer.produce(
                topic=self.topic,
                value=event,
                key=consumer_group
            )

            # Small delay to avoid overwhelming producer buffer
            time.sleep(0.1)

        # Phase 2: Sustained elevated rate to maintain lag
        sustained_duration = 80  # seconds
        sustained_rate = 500  # msg/s
        interval = 1.0 / sustained_rate

        end_time = time.time() + sustained_duration

        while time.time() < end_time:
            event = create_event(
                consumer_group=consumer_group,
                topic=target_topic,
                scenario_name=self.name,
                scenario_id=self.scenario_id,
                message_count=1,
                phase="sustained"
            )

            self.producer.produce(
                topic=self.topic,
                value=event,
                key=consumer_group
            )

            time.sleep(interval)
