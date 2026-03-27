import time
import sys
sys.path.append('..')
from scenarios.base_scenario import BaseScenario
from event_schema import create_event


class RebalanceStormScenario(BaseScenario):
    """
    Rebalance Storm: Simulates repeated consumer group rebalances.

    Publishes events with rebalance trigger markers to simulate a consumer group
    experiencing 10+ rebalances in a 2-minute window. This can happen due to:
    - Network instability
    - Consumer crashes/restarts
    - Session timeout configuration issues
    """

    @property
    def name(self) -> str:
        return "rebalance_storm"

    @property
    def description(self) -> str:
        return "Triggers 15+ consumer group rebalances in 2-minute window"

    @property
    def duration_seconds(self) -> int:
        return 120

    def _execute(self, consumer_group: str, target_topic: str, **kwargs):
        """
        Inject rebalance storm pattern.

        Pattern:
        - Trigger 15 rebalances over 120 seconds (~1 every 8 seconds)
        - Each rebalance event marks a consumer group state change
        - Publishes baseline events between rebalances
        """
        num_rebalances = kwargs.get("num_rebalances", 15)
        rebalance_interval = self.duration_seconds / num_rebalances

        for rebalance_num in range(num_rebalances):
            # Publish rebalance event
            rebalance_event = create_event(
                consumer_group=consumer_group,
                topic=target_topic,
                scenario_name=self.name,
                scenario_id=self.scenario_id,
                message_count=0,
                event_subtype="rebalance_trigger",
                rebalance_number=rebalance_num + 1,
                rebalance_reason="simulated_instability"
            )

            self.producer.produce(
                topic=self.topic,
                value=rebalance_event,
                key=consumer_group
            )

            # Publish baseline events between rebalances (simulate normal traffic)
            interval_end = time.time() + rebalance_interval
            while time.time() < interval_end:
                baseline_event = create_event(
                    consumer_group=consumer_group,
                    topic=target_topic,
                    scenario_name=self.name,
                    scenario_id=self.scenario_id,
                    message_count=1
                )

                self.producer.produce(
                    topic=self.topic,
                    value=baseline_event,
                    key=consumer_group
                )

                time.sleep(0.1)  # 10 msg/s baseline
