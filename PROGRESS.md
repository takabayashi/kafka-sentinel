# iFood Kafka Anomaly Detection - Build Progress

## ✅ Completed

### 1. Infrastructure (Terraform)
**Status:** Deployed to Confluent Cloud
**Location:** `infra/terraform/`

- ✅ All 7 Kafka topics provisioned:
  - `simulator_events` (3 partitions)
  - `metrics_source` (6 partitions)
  - `metrics_flattened` (6 partitions)
  - `velocity_anomaly_alerts` (3 partitions)
  - `enriched_alerts` (3 partitions)
  - `agent_memory` (6 partitions, **COMPACTED**)
  - `alert_feedback` (1 partition)

**Cluster:** `lkc-nnrvx6` (Standard tier)
**Bootstrap:** `pkc-921jm.us-east-2.aws.confluent.cloud:9092`

### 2. Data Simulator (Python/Flask)
**Status:** Running on http://localhost:5001
**Location:** `simulator/`

**Features:**
- ✅ Flask API with 9 endpoints
- ✅ Free Producer mode (continuous baseline events)
- ✅ 4 anomaly scenarios:
  - `lag_spike` - 45K message burst (90s)
  - `consumer_slow` - degraded throughput (120s)
  - `rebalance_storm` - 15 rebalances (120s)
  - `hot_partition` - partition skew (90s)
- ✅ Kafka connectivity verified
- ✅ Events publishing to `simulator_events` topic

**Virtual Environment:** `simulator/venv/`

**Test Commands:**
```bash
# Start simulator
cd simulator
source venv/bin/activate
python main.py

# Trigger lag spike
curl -X POST http://localhost:5001/simulator/scenario/lag_spike -H "Content-Type: application/json" -d '{}'

# Check status
curl http://localhost:5001/simulator/status
```

## 🔄 In Progress

None

## 📋 Next Steps (from AGENTS.md)

### 3. Velocity Monitor (Python)
**Priority:** Next to build
**Purpose:** Polls Confluent Cloud APIs every ~10s, computes metrics, publishes to `metrics_source`

**Key Requirements:**
- Poll Consumer Group API (NOT Metrics API - too slow)
- Poll Cluster Metadata API
- Compute per consumer group:
  - Current lag
  - Read speed (msg/s)
  - Write speed (msg/s)
  - Time-to-catch-up
  - 24h average lag
  - Partition skew score
- Publish to `metrics_source` topic

### 4. Flink Pipeline (Flink SQL)
7 SQL statements to deploy on Confluent Cloud:
1. Formatting step
2. ARIMA - Lag Trending Up
3. ARIMA - Speed Trending Down
4. ARIMA - Hot Partition Prediction
5. Threshold - Consumer Group Inactive
6. Threshold - Rebalance Storm
7. Correlation Engine

### 5. AI Agent (Confluent Intelligence)
Enriches alerts with natural language diagnosis

### 6. Dashboard UI (React)
Three panels: Metrics View, Alert Feed, Simulator Controls

## Configuration Files

- `config/kafka-config.json` - Terraform outputs (topics, endpoints)
- `simulator/.env` - Kafka API credentials
- `infra/terraform/terraform.tfvars` - Cloud + Kafka API keys

## Demo Flow Test

1. ✅ Engineer clicks "Lag Spike" in Dashboard → calls `/simulator/scenario/lag_spike`
2. ✅ Simulator publishes 45K events to `simulator_events`
3. ⏳ Velocity Monitor detects lag → publishes to `metrics_source`
4. ⏳ Flink ARIMA detects anomaly → publishes to `velocity_anomaly_alerts`
5. ⏳ AI Agent enriches → publishes to `enriched_alerts`
6. ⏳ Dashboard shows alert: "Lag spiked from 2K to 45K. Producer burst detected..."

**Time to alert:** Target 30-60 seconds end-to-end
