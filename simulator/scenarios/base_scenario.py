from abc import ABC, abstractmethod
import uuid
import logging
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseScenario(ABC):
    """Abstract base class for anomaly injection scenarios"""

    def __init__(self, producer, kafka_config):
        self.producer = producer
        self.kafka_config = kafka_config
        self.topic = kafka_config["simulator_topic"]
        self.scenario_id = None
        self.start_time = None

    @property
    @abstractmethod
    def name(self) -> str:
        """Scenario name (e.g., 'lag_spike')"""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description of what this scenario does"""
        pass

    @property
    @abstractmethod
    def duration_seconds(self) -> int:
        """How long the scenario runs"""
        pass

    def run(
        self,
        consumer_group: str = "checkout-service",
        target_topic: str = "orders",
        **kwargs
    ) -> str:
        """
        Execute the scenario.

        Args:
            consumer_group: Simulated consumer group name
            target_topic: Target Kafka topic
            **kwargs: Scenario-specific parameters

        Returns:
            scenario_id for tracking
        """
        self.scenario_id = f"{self.name}_{uuid.uuid4().hex[:8]}"
        self.start_time = datetime.utcnow()

        logger.info(
            f"Starting scenario '{self.name}' (ID: {self.scenario_id}) "
            f"for {consumer_group}/{target_topic}"
        )

        try:
            self._execute(consumer_group, target_topic, **kwargs)
            logger.info(f"Scenario '{self.name}' (ID: {self.scenario_id}) completed")
        except Exception as e:
            logger.error(f"Scenario '{self.name}' failed: {e}", exc_info=True)
            raise
        finally:
            self.producer.flush()

        return self.scenario_id

    @abstractmethod
    def _execute(self, consumer_group: str, target_topic: str, **kwargs):
        """
        Scenario-specific implementation.

        Subclasses must implement this method to define the anomaly pattern.
        """
        pass
