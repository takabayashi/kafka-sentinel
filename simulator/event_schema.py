import json
from datetime import datetime
from typing import Dict, Any, Optional


def create_event(
    consumer_group: str,
    topic: str,
    scenario_name: Optional[str] = None,
    scenario_id: Optional[str] = None,
    message_count: int = 1,
    **kwargs
) -> str:
    """
    Create a JSON event for the simulator_events topic.

    Args:
        consumer_group: Simulated consumer group name
        topic: Target Kafka topic
        scenario_name: Name of scenario (e.g., 'lag_spike', 'consumer_slow')
        scenario_id: Unique scenario run identifier
        message_count: Number of messages in this batch
        **kwargs: Additional fields to include in event

    Returns:
        JSON string ready for Kafka
    """
    event = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "consumer_group": consumer_group,
        "topic": topic,
        "message_count": message_count,
        **kwargs
    }

    if scenario_name:
        event["scenario_name"] = scenario_name
        event["event_type"] = "anomaly_injection"
    else:
        event["event_type"] = "baseline_event"

    if scenario_id:
        event["scenario_id"] = scenario_id

    return json.dumps(event)
