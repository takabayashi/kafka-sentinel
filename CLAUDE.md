# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **real-time Kafka infrastructure anomaly detection system** for the iFood Platform Team demo. The system uses Confluent Cloud, Flink SQL, and Confluent Intelligence to:
- Detect anomalies in Kafka consumer group performance
- Enrich alerts with AI-generated natural language diagnoses
- Reduce MTTR by explaining problems without requiring manual investigation

## Architecture

```
Data Simulator → Velocity Monitor → [metrics_source] → Flink Pipeline → [velocity_anomaly_alerts] → AI Agent → [enriched_alerts] → Dashboard UI
                                                                                                          ↕
                                                                                                   [agent_memory] (compacted)
```

### Data Flow
1. **Velocity Monitor** polls Confluent Cloud APIs every ~10s, publishes metrics to `metrics_source`
2. **Flink Pipeline** runs anomaly detection (ARIMA + thresholds), publishes to `velocity_anomaly_alerts`
3. **AI Agent** enriches alerts with diagnosis, publishes to `enriched_alerts` and updates `agent_memory`
4. **Dashboard UI** displays real-time metrics and AI-enriched alerts
5. **Data Simulator** injects synthetic anomalies for demo environments

## Key Technical Decisions

### Use Consumer Group API, NOT Metrics API
The Confluent Cloud Metrics API has multi-minute delay. For real-time detection, use:
- **Consumer Group API** for lag, offset commits, consumer state
- **Cluster Metadata API** for topic/partition metadata

### Compacted Topic for Agent Memory
`agent_memory` topic uses `cleanup.policy=compact` with `consumer_group` as key. This keeps historical context inside Kafka without external databases.

### Scenario-Based Simulator
ARIMA models need historical data to establish baselines. The simulator provides:
- **Free Producer**: continuous event generation with configurable throughput
- **Scenario Buttons**: pre-defined anomaly patterns (lag_spike, consumer_slow, rebalance_storm, hot_partition)

## Kafka Topics

| Topic | Type | Key | Description |
|---|---|---|---|
| `simulator_events` | Regular | - | Synthetic events from simulator |
| `metrics_source` | Regular | consumer_group | Raw metrics from Velocity Monitor |
| `metrics_flattened` | Regular | consumer_group | Normalized metrics (post-Flink formatting) |
| `velocity_anomaly_alerts` | Regular | consumer_group | Alerts from Flink rules |
| `enriched_alerts` | Regular | alert_id | Alerts + AI diagnosis |
| `agent_memory` | **Compacted** | consumer_group | Historical context for AI Agent |
| `alert_feedback` | Regular | alert_id | Thumbs up/down from engineers |

## Anomaly Detection Rules

Flink pipeline runs these detection rules in sequence:

1. **Formatting Step** (SQL normalization)
2. **ARIMA - Lag Trending Up** (ML_DETECT_ANOMALIES)
3. **ARIMA - Speed Trending Down** (ML_DETECT_ANOMALIES)
4. **ARIMA - Hot Partition Prediction** (ML_DETECT_ANOMALIES)
5. **Threshold - Consumer Group Inactive** (no offset commit > 60s)
6. **Threshold - Rebalance Storm** (> N rebalances in window)
7. **Correlation Engine** (2+ groups on same topic degrade together)

## Component Structure

### Velocity Monitor (Python)
- Polls Consumer Group API and Cluster Metadata API
- Computes: lag, read/write speed, time-to-catch-up, 24h avg lag, partition skew
- Publishes to `metrics_source`
- Dependencies: `confluent-kafka`, `requests`

### Data Simulator (Python)
- Two modes: free producer + scenario buttons
- Scenarios: lag_spike, consumer_slow, rebalance_storm, hot_partition
- Essential for demo environments without historical data
- Dependencies: `confluent-kafka`

### Flink Pipeline (Flink SQL)
- Deployed on Confluent Cloud
- Uses ML_DETECT_ANOMALIES for ARIMA models
- All statements read from `metrics_flattened`, write to `velocity_anomaly_alerts`

### AI Agent (Confluent Intelligence)
- Triggered for every new alert in `velocity_anomaly_alerts`
- Fetches historical context from `agent_memory`
- Checks for correlated alerts (same topic, last 5 min)
- Calls AI model via MCP Server
- Generates: diagnosis + severity + recommended action
- Updates `agent_memory` with new context

### Dashboard UI (React)
- Three panels: Metrics View, Alert Feed, Simulator Panel
- WebSocket connection for real-time updates
- Feedback publishes to `alert_feedback`

## Suggested Build Order

1. **Infrastructure** (`infra/topics.tf`) - Create Kafka topics on Confluent Cloud
2. **Simulator** (`simulator/`) - Generate data flow
3. **Velocity Monitor** (`velocity-monitor/`) - Start publishing metrics
4. **Flink Formatting** (`flink/01_formatting.sql`) - Get `metrics_flattened` working
5. **First ARIMA Rule** (`flink/02_arima_lag_trending_up.sql`) - Validate anomaly detection
6. **Dashboard Metrics View** (`dashboard/`) - Validate data visualization
7. **Remaining Flink Rules** - Add detection rules incrementally
8. **AI Agent** (`ai-agent/`) - Wire up Confluent Intelligence
9. **Dashboard Alert Feed + Simulator Panel** - Complete UI

## Demo Flow

1. Engineer presses "Lag Spike" button in Dashboard
2. Simulator injects anomaly pattern
3. ~30s later: alert appears with AI diagnosis like:
   - "Consumer group `checkout-service` lag spiked from 2K to 45K messages. Write speed to topic `orders` increased 300% in last 5min while consumer throughput remained flat. **Recommended**: Scale consumer group or optimize message processing."
4. Engineer provides thumbs up/down feedback → published to `alert_feedback`

## Important Notes

- This is a demo system optimized for showing real-time anomaly detection with AI enrichment
- Focus on clarity and "wow moments" over production-grade error handling
- The cross-group correlation detection (identifying producer/broker issues vs consumer issues) is the highest-impact demo moment for platform teams
