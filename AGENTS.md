# Kafka Anomaly Detection — Confluent Intelligence
## Project Context for Claude Code

This project is a **real-time Kafka infrastructure anomaly detection system** built for the **iFood Platform Team** demo. It uses Confluent Cloud, Flink for Confluent Cloud, and Confluent Intelligence to automatically detect performance degradations and enrich alerts with AI-generated diagnoses.

The goal is to show the iFood platform team that instead of just *detecting* a problem, the system *explains* it in natural language and suggests an action — reducing MTTR without opening Grafana.

---

## Architecture Overview

```
Data Simulator → Velocity Monitor → [metrics_source] → Flink Pipeline → [velocity_anomaly_alerts] → AI Agent → [enriched_alerts] → Dashboard UI
                                                                                                          ↕
                                                                                                   [agent_memory] (compacted)
```

---

## Components

### 1. Velocity Monitor (Python)
- Polls **Consumer Group API** and **Cluster Metadata API** every ~10s
- ⚠️ Do NOT use the Confluent Cloud Metrics API — it has a multi-minute delay and is not suitable for real-time detection
- Computes per consumer group: current lag, read speed (msg/s), write speed (msg/s), time-to-catch-up (seconds), 24h average lag, partition skew score
- Publishes to `metrics_source` topic

### 2. Data Simulator (Python)
Two modes, both triggerable from the Dashboard UI:
- **Free Producer** — continuous event generator with configurable throughput, consumer count, partitions
- **Scenario Buttons** — injects pre-defined anomaly patterns:
  - `lag_spike` — rapidly increases consumer lag
  - `consumer_slow` — slows down consumption rate
  - `rebalance_storm` — triggers repeated rebalances
  - `hot_partition` — creates uneven partition load
- Essential for demo environments without historical data for ARIMA training

### 3. Flink Pipeline (Flink SQL on Confluent Cloud)
Five statements in sequence:

| Statement | Input | Output | Method |
|---|---|---|---|
| Formatting Step | metrics_source | metrics_flattened | SQL normalization |
| ARIMA — Lag Trending Up | metrics_flattened | velocity_anomaly_alerts | ML_DETECT_ANOMALIES |
| ARIMA — Speed Trending Down | metrics_flattened | velocity_anomaly_alerts | ML_DETECT_ANOMALIES |
| ARIMA — Hot Partition Prediction | metrics_flattened | velocity_anomaly_alerts | ML_DETECT_ANOMALIES |
| Threshold — Consumer Group Inactive | metrics_flattened | velocity_anomaly_alerts | No offset commit > 60s |
| Threshold — Rebalance Storm | metrics_flattened | velocity_anomaly_alerts | > N rebalances in window |
| Correlation Engine | metrics_flattened | velocity_anomaly_alerts | 2+ groups same topic degrade together |

### 4. AI Agent (Confluent Intelligence via MCP)
Triggered automatically for every new alert in `velocity_anomaly_alerts`.

Flow:
1. Read alert (consumer group, topic, partition, anomaly type, current value)
2. Fetch historical context from `agent_memory` (compacted topic, keyed by consumer group)
3. Check for correlated alerts on same topic within last 5 minutes
4. Call AI model via MCP Server with full context
5. Generate: natural language diagnosis + severity (LOW/MEDIUM/HIGH/CRITICAL) + recommended action
6. Publish enriched alert to `enriched_alerts`
7. Update `agent_memory` with new context

### 5. Dashboard UI (React)
Three panels:
- **Metrics View** — real-time charts (lag, read/write speed, partition skew) per consumer group/topic
- **Alert Feed** — AI-enriched alerts with diagnosis, severity badge, thumbs up/down feedback
- **Simulator Panel** — sliders for free producer + scenario buttons

Feedback publishes to `alert_feedback` topic.

---

## Kafka Topics

| Topic | Type | Key | Description |
|---|---|---|---|
| `simulator_events` | Regular | - | Synthetic events from simulator |
| `metrics_source` | Regular | consumer_group | Raw metrics from Velocity Monitor |
| `metrics_flattened` | Regular | consumer_group | Normalized metrics post-Flink formatting |
| `velocity_anomaly_alerts` | Regular | consumer_group | Alerts from Flink rules |
| `enriched_alerts` | Regular | alert_id | Alerts + AI diagnosis |
| `agent_memory` | **Compacted** | consumer_group | Historical context for AI Agent |
| `alert_feedback` | Regular | alert_id | Thumbs up/down from engineers |

---

## Anomaly Detection Rules

| Rule | Method | Trigger | Severity |
|---|---|---|---|
| Lag Trending Up | ARIMA | Lag > baseline OR > 110% of 24h avg | HIGH |
| Speed Trending Down | ARIMA | Read speed anomaly + worsening trend | HIGH |
| Hot Partition Prediction | ARIMA | Partition skew score exceeds threshold | MEDIUM |
| Consumer Group Inactive | Threshold | No offset commit > 60s | CRITICAL |
| Rebalance Storm | Threshold | > N rebalances in rolling window | HIGH |
| Cross-Group Correlation | Correlation | 2+ groups on same topic degrade together | CRITICAL |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Velocity Monitor | Python + confluent-kafka + requests |
| Data Simulator | Python + confluent-kafka |
| Stream Processing | Flink SQL on Confluent Cloud (ML_DETECT_ANOMALIES) |
| AI Agent | Confluent Intelligence via MCP Server |
| Dashboard UI | React + WebSocket |
| Messaging | Confluent Cloud (Kafka) |
| Schema Management | Confluent Schema Registry (Avro) |
| Historical Context | Compacted Kafka topic (agent_memory) |

---

## Suggested Project Structure

```
/
├── AGENTS.md                  ← this file
├── velocity-monitor/          ← Python service
│   ├── main.py
│   ├── consumer_group_api.py
│   ├── cluster_metadata_api.py
│   └── metrics_publisher.py
├── simulator/                 ← Python simulator
│   ├── free_producer.py
│   └── scenarios/
│       ├── lag_spike.py
│       ├── consumer_slow.py
│       ├── rebalance_storm.py
│       └── hot_partition.py
├── flink/                     ← Flink SQL statements
│   ├── 01_formatting.sql
│   ├── 02_arima_lag_trending_up.sql
│   ├── 03_arima_speed_trending_down.sql
│   ├── 04_arima_hot_partition.sql
│   ├── 05_threshold_inactive.sql
│   ├── 06_threshold_rebalance.sql
│   └── 07_correlation_engine.sql
├── ai-agent/                  ← Confluent Intelligence / MCP
│   ├── agent.py
│   ├── memory.py
│   └── prompts/
│       └── diagnosis.txt
├── dashboard/                 ← React UI
│   ├── src/
│   │   ├── components/
│   │   │   ├── MetricsView.jsx
│   │   │   ├── AlertFeed.jsx
│   │   │   └── SimulatorPanel.jsx
│   │   └── App.jsx
│   └── package.json
└── infra/
    └── topics.tf              ← Terraform for topic creation
```

---

## Demo Wow Moments

1. **Lag Spike** → press button → ~30s → alert appears with natural language diagnosis
2. **Rebalance Storm** → agent correlates with app instability, identifies consumer-level cause
3. **Cross-Group Correlation** → two scenarios on same topic → agent identifies producer/broker as root cause (not the consumers) — highest impact moment for a platform team
4. **Recurring Pattern** → after multiple scenarios, agent surfaces: *"This consumer group has spiked 3 times in the last hour — consider reviewing consumer performance or increasing partition count"*

---

## What to Build First (Suggested Order)

1. **`infra/topics.tf`** — create all Kafka topics on Confluent Cloud
2. **`simulator/`** — need data flowing before anything else works
3. **`velocity-monitor/`** — start polling and publishing metrics
4. **`flink/01_formatting.sql`** — get metrics_flattened working
5. **`flink/02_arima_lag_trending_up.sql`** — first anomaly detection
6. **`dashboard/`** — basic metrics view to validate data is flowing
7. **Remaining Flink statements** — add rules one by one
8. **`ai-agent/`** — wire up Confluent Intelligence last, once alerts are stable
9. **Dashboard alert feed + simulator panel** — complete the UI

---

## Key Decisions Made

- **Consumer Group API over Metrics API** — Metrics API has multi-minute delay; Consumer Group API is real-time
- **Compacted topic for agent memory** — keeps historical context inside Confluent Cloud, no external DB needed
- **Python for Velocity Monitor** — simpler for demo; Java is also fine (colleague's version used Java)
- **Scenarios-first simulator** — ARIMA needs historical data to learn; simulator injects synthetic history for demo environments
- **Thumbs up/down feedback** — published to Kafka for future fine-tuning, closes the data loop

---

*Generated during architecture planning session. Continue from "What to Build First" section.*
