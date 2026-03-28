# Extending the Anomaly Detection Demo

This guide shows how contributors can extend the demo with new features.

## 🎯 Extension Points

### 1. Adding New Anomaly Scenarios

New scenarios can be added to simulate different types of failures.

**Location:** `simulator/scenarios/`

**Example:** Create `simulator/scenarios/network_partition.py`

```python
from .base_scenario import BaseScenario

class NetworkPartitionScenario(BaseScenario):
    def __init__(self, producer, kafka_config):
        super().__init__(
            name="network_partition",
            description="Simulates network partition between consumer and broker",
            duration_seconds=60
        )
        self.producer = producer
        self.kafka_config = kafka_config

    def run(self, consumer_group='checkout-service', target_topic='orders', **kwargs):
        """
        Simulate network partition by:
        1. Stop producing to simulate broker unavailability
        2. Burst produce after recovery
        3. Mark events with anomaly_marker for tracking
        """
        scenario_id = self.generate_scenario_id()

        # Phase 1: Silence (30s) - simulate partition
        logger.info(f"[{scenario_id}] Phase 1: Network partition (no events)")
        time.sleep(30)

        # Phase 2: Recovery burst (30s)
        logger.info(f"[{scenario_id}] Phase 2: Network recovered, burst mode")
        for i in range(300):  # 10 events/sec for 30s
            event = self.create_event(
                event_type='order_placed',
                consumer_group=consumer_group,
                anomaly_marker=f'network_partition_{scenario_id}'
            )
            self.producer.send(target_topic, event)
            time.sleep(0.1)

        return scenario_id
```

**Register in:** `simulator/main.py`

```python
from scenarios import NetworkPartitionScenario

scenarios = {
    "lag_spike": LagSpikeScenario(producer_wrapper, kafka_topic_config),
    "network_partition": NetworkPartitionScenario(producer_wrapper, kafka_topic_config),  # NEW
}
```

**Add UI button:** `dashboard/frontend/src/components/SimulatorPanel.jsx`

```javascript
const scenarios = [
  // ... existing scenarios
  {
    id: 'network_partition',
    name: 'Network Partition',
    description: 'Simulates network split between consumer and broker',
    icon: '🌐',
    color: '#06b6d4'
  }
];
```

---

### 2. Adding New Flink Detection Rules

Custom anomaly detection logic can be added as new Flink SQL statements.

**Location:** `flink/`

**Example:** Create `flink/08_cpu_spike_detection.sql`

```sql
-- Detect when consumer group CPU usage spikes above normal

CREATE TABLE cpu_metrics_source (
  consumer_group STRING,
  cpu_percent DOUBLE,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'cpu_metrics',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

-- Calculate moving average and detect spikes
INSERT INTO velocity_anomaly_alerts
SELECT
  consumer_group,
  'CPU_SPIKE' as anomaly_type,
  'critical' as severity,
  CAST(cpu_percent AS STRING) as metric_value,
  event_time as timestamp
FROM (
  SELECT
    consumer_group,
    cpu_percent,
    event_time,
    AVG(cpu_percent) OVER (
      PARTITION BY consumer_group
      ORDER BY event_time
      ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
    ) as avg_cpu
  FROM cpu_metrics_source
)
WHERE cpu_percent > (avg_cpu * 2);  -- Spike = 2x above average
```

**Deploy:**
```bash
# Add to Makefile
flink-deploy-cpu-spike:
	@echo "Deploying CPU spike detection rule..."
	flink run -d flink/08_cpu_spike_detection.sql
```

---

### 3. Adding New Metrics Sources

Extend the Velocity Monitor to collect new metrics.

**Location:** `velocity-monitor/metrics_collectors/`

**Example:** Create `velocity-monitor/metrics_collectors/jvm_collector.py`

```python
import requests
from typing import Dict, Any

class JVMMetricsCollector:
    """Collects JVM metrics from consumer applications via JMX/Prometheus"""

    def __init__(self, prometheus_url: str):
        self.prometheus_url = prometheus_url

    def collect(self, consumer_group: str) -> Dict[str, Any]:
        """
        Collect JVM metrics for a consumer group.

        Returns:
        {
            'heap_used_mb': 512,
            'heap_max_mb': 1024,
            'gc_count': 150,
            'gc_time_ms': 2500,
            'thread_count': 45
        }
        """
        query = f'jvm_memory_used_bytes{{consumer_group="{consumer_group}"}}'
        response = requests.get(
            f'{self.prometheus_url}/api/v1/query',
            params={'query': query}
        )

        data = response.json()

        return {
            'consumer_group': consumer_group,
            'heap_used_mb': data['data']['result'][0]['value'][1] / 1024 / 1024,
            'timestamp': datetime.utcnow().isoformat()
        }
```

**Integrate:** `velocity-monitor/monitor.py`

```python
from metrics_collectors.jvm_collector import JVMMetricsCollector

jvm_collector = JVMMetricsCollector(prometheus_url='http://prometheus:9090')

# In polling loop
for group in consumer_groups:
    consumer_metrics = get_consumer_group_metrics(group)
    jvm_metrics = jvm_collector.collect(group)  # NEW

    # Merge and publish
    metrics = {**consumer_metrics, **jvm_metrics}
    publish_metrics(metrics)
```

---

### 4. Adding Dashboard Panels

New visualization panels can be added to the dashboard.

**Location:** `dashboard/frontend/src/components/`

**Example:** Create `dashboard/frontend/src/components/JVMMetrics.jsx`

```javascript
import { useState, useEffect, memo } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import './JVMMetrics.css';

const JVMMetrics = memo(function JVMMetrics({ ws }) {
  const [jvmData, setJvmData] = useState([]);

  useEffect(() => {
    if (!ws) return;

    const handleMessage = (event) => {
      const message = JSON.parse(event.data);

      if (message.topic === 'jvm_metrics') {
        setJvmData((prev) => {
          const updated = [...prev, message.data.value];
          return updated.slice(-50); // Keep last 50 data points
        });
      }
    };

    ws.addEventListener('message', handleMessage);
    return () => ws.removeEventListener('message', handleMessage);
  }, [ws]);

  return (
    <div className="jvm-metrics-panel">
      <h2>JVM Metrics</h2>

      <ResponsiveContainer width="100%" height={200}>
        <LineChart data={jvmData}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="timestamp" />
          <YAxis />
          <Tooltip />
          <Line type="monotone" dataKey="heap_used_mb" stroke="#8884d8" name="Heap (MB)" />
          <Line type="monotone" dataKey="gc_time_ms" stroke="#82ca9d" name="GC Time (ms)" />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
});

export default JVMMetrics;
```

**Add to dashboard:** `dashboard/frontend/src/App.jsx`

```javascript
import JVMMetrics from './components/JVMMetrics';

// In render:
<div className="dashboard-grid">
  <div className="panel jvm-panel">
    <JVMMetrics ws={ws} />  {/* NEW */}
  </div>
  {/* ... existing panels */}
</div>
```

---

### 5. Adding Alert Enrichment Rules

Custom AI enrichment logic can be added to the agent.

**Location:** `ai-agent/enrichment_rules/`

**Example:** Create `ai-agent/enrichment_rules/correlation_detector.py`

```python
class CorrelationDetector:
    """Detects correlated anomalies across multiple consumer groups"""

    def __init__(self, kafka_consumer, memory_topic='agent_memory'):
        self.consumer = kafka_consumer
        self.memory_topic = memory_topic

    def check_correlations(self, alert: dict) -> dict:
        """
        Check if this alert correlates with others in the last 5 minutes.

        Returns enrichment data:
        {
            'correlated_groups': ['group-1', 'group-2'],
            'correlation_type': 'same_topic_degradation',
            'root_cause_probability': 'broker_issue'
        }
        """
        topic = alert.get('topic')
        timestamp = alert.get('timestamp')

        # Query recent alerts for the same topic
        recent_alerts = self.get_recent_alerts(
            topic=topic,
            time_window_minutes=5
        )

        if len(recent_alerts) >= 3:
            # Multiple groups affected = likely broker/producer issue
            return {
                'correlated_groups': [a['consumer_group'] for a in recent_alerts],
                'correlation_type': 'same_topic_degradation',
                'root_cause_probability': 'broker_or_producer_issue',
                'recommended_action': f'Check broker health and producer for topic {topic}'
            }

        return {}
```

**Integrate:** `ai-agent/agent.py`

```python
from enrichment_rules.correlation_detector import CorrelationDetector

correlation_detector = CorrelationDetector(kafka_consumer)

def enrich_alert(alert):
    # Existing enrichment
    diagnosis = generate_diagnosis(alert)

    # NEW: Check correlations
    correlations = correlation_detector.check_correlations(alert)

    return {
        **alert,
        'diagnosis': diagnosis,
        'correlations': correlations  # NEW
    }
```

---

### 6. Adding Integration Connectors

Integrate with external systems like Slack, PagerDuty, etc.

**Location:** `integrations/`

**Example:** Create `integrations/slack_notifier.py`

```python
import requests
from typing import Dict, Any

class SlackNotifier:
    """Sends enriched alerts to Slack"""

    def __init__(self, webhook_url: str, channel: str = '#platform-alerts'):
        self.webhook_url = webhook_url
        self.channel = channel

    def send_alert(self, alert: Dict[str, Any]):
        """
        Format and send alert to Slack.

        Alert format:
        {
            'consumer_group': 'checkout-service',
            'anomaly_type': 'LAG_SPIKE',
            'severity': 'critical',
            'diagnosis': '...',
            'recommended_action': '...'
        }
        """
        color = {
            'critical': '#ef4444',
            'high': '#f59e0b',
            'medium': '#3b82f6',
            'low': '#6b7280'
        }.get(alert.get('severity', 'medium'))

        message = {
            'channel': self.channel,
            'attachments': [{
                'color': color,
                'title': f"🚨 {alert['anomaly_type']} - {alert['consumer_group']}",
                'fields': [
                    {
                        'title': 'Diagnosis',
                        'value': alert.get('diagnosis', 'No diagnosis available'),
                        'short': False
                    },
                    {
                        'title': 'Recommended Action',
                        'value': alert.get('recommended_action', 'Investigate manually'),
                        'short': False
                    },
                    {
                        'title': 'Severity',
                        'value': alert.get('severity', 'unknown').upper(),
                        'short': True
                    },
                    {
                        'title': 'Topic',
                        'value': alert.get('topic', 'N/A'),
                        'short': True
                    }
                ],
                'footer': 'Kafka Anomaly Detection',
                'ts': int(datetime.now().timestamp())
            }]
        }

        response = requests.post(self.webhook_url, json=message)
        response.raise_for_status()
```

**Usage:** Add to alert pipeline

```python
# In ai-agent or dashboard backend
slack = SlackNotifier(webhook_url=os.getenv('SLACK_WEBHOOK_URL'))

# When enriched alert is ready
def on_enriched_alert(alert):
    # Existing: publish to Kafka
    publish_to_kafka('enriched_alerts', alert)

    # NEW: Send to Slack
    if alert.get('severity') in ['critical', 'high']:
        slack.send_alert(alert)
```

---

## 🏗️ Architecture Patterns

### Plugin System

Create a plugin interface for easy extensibility:

```python
# core/plugin_interface.py
from abc import ABC, abstractmethod

class AnomalyDetectorPlugin(ABC):
    """Base class for anomaly detection plugins"""

    @abstractmethod
    def name(self) -> str:
        """Plugin name"""
        pass

    @abstractmethod
    def detect(self, metrics: dict) -> dict:
        """
        Analyze metrics and return anomaly if detected.

        Returns:
        {
            'is_anomaly': True/False,
            'anomaly_type': 'LAG_SPIKE',
            'severity': 'critical',
            'confidence': 0.95
        }
        """
        pass

# Example plugin
class CustomLagDetector(AnomalyDetectorPlugin):
    def name(self):
        return "custom_lag_detector"

    def detect(self, metrics):
        lag = metrics.get('lag', 0)
        if lag > 10000:
            return {
                'is_anomaly': True,
                'anomaly_type': 'CUSTOM_LAG_SPIKE',
                'severity': 'critical',
                'confidence': 0.98
            }
        return {'is_anomaly': False}
```

### Configuration-Driven Extensions

Use YAML config files for declarative extensions:

```yaml
# config/extensions.yaml
scenarios:
  - name: network_partition
    class: scenarios.NetworkPartitionScenario
    enabled: true
    params:
      duration_seconds: 60

detectors:
  - name: custom_lag
    class: detectors.CustomLagDetector
    enabled: true
    thresholds:
      critical: 10000
      high: 5000

integrations:
  - name: slack
    class: integrations.SlackNotifier
    enabled: true
    config:
      webhook_url: ${SLACK_WEBHOOK_URL}
      channel: "#platform-alerts"
      severity_filter: ["critical", "high"]
```

---

## 📚 Contribution Workflow

1. **Fork the repository**
2. **Create feature branch:** `git checkout -b feature/new-scenario`
3. **Add your extension** following patterns above
4. **Write tests:** `tests/test_new_scenario.py`
5. **Update docs:** Add to `EXTENDING.md`
6. **Submit PR** with description and example usage

---

## 🧪 Testing Extensions

```bash
# Test new scenario
cd simulator
python -m pytest tests/test_new_scenario.py

# Test Flink rule locally
flink run --local flink/08_cpu_spike_detection.sql

# Test UI component
cd dashboard/frontend
npm test -- JVMMetrics.test.jsx
```

---

## 📖 Documentation Template

When adding a new extension, include:

```markdown
## Extension Name

**What it does:** Brief description

**Use case:** When to use this extension

**Installation:**
1. Step 1
2. Step 2

**Configuration:**
```yaml
# Example config
```

**Example:**
```python
# Code example
```

**Testing:**
```bash
# How to test
```

**Author:** @username
**Status:** experimental | stable | deprecated
```

---

## 💡 Ideas for Future Extensions

- **Auto-scaling trigger**: Automatically scale consumer groups based on lag
- **Cost analyzer**: Estimate infrastructure cost impact of anomalies
- **Playbook executor**: Auto-remediation based on alert type
- **Multi-cluster support**: Monitor multiple Kafka clusters
- **Historical replay**: Replay past anomalies for training
- **A/B testing framework**: Test different detection algorithms
- **Custom dashboards**: User-defined dashboard layouts
- **Alert routing**: Route alerts based on consumer group ownership
- **SLO tracking**: Track SLO violations and trends
- **Capacity planning**: Predict when to add partitions/consumers
