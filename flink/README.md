# Flink SQL Pipeline

This directory contains Flink SQL statements for real-time anomaly detection on Kafka consumer group metrics.

## Overview

The pipeline uses **Confluent Cloud Flink** with ML-powered anomaly detection (ARIMA models) and threshold-based rules to identify infrastructure issues:

- **ARIMA Models**: Detect trending anomalies (lag increasing, speed decreasing, partition skew)
- **Threshold Rules**: Detect discrete events (consumer crashes, rebalancing storms)
- **Correlation Engine**: Distinguish producer/broker issues from consumer issues

## Prerequisites

- **Confluent Cloud Standard Cluster** (ML_DETECT_ANOMALIES requires Standard tier or higher)
- **Flink Compute Pool** provisioned in your environment
- **Schema Registry** enabled on your cluster
- **velocity-monitor** running and publishing to `metrics_source` topic

## Deployment Order

Execute these SQL statements in Confluent Cloud Flink Console **in order**:

### 1. Formatting (REQUIRED FIRST)
```sql
-- File: 01_formatting.sql
-- Creates: metrics_source (source), metrics_flattened (sink)
-- Purpose: Normalize raw metrics and compute derived fields
```

### 2. ARIMA - Lag Trending Up
```sql
-- File: 02_arima_lag_trending_up.sql
-- Creates: velocity_anomaly_alerts (sink)
-- Purpose: Detect consumer lag increasing over time
```

### 3. ARIMA - Speed Trending Down
```sql
-- File: 03_arima_speed_trending_down.sql
-- Purpose: Detect consumer throughput degradation
```

### 4. ARIMA - Hot Partition
```sql
-- File: 04_arima_hot_partition.sql
-- Purpose: Detect partition skew anomalies (hot partitions)
```

### 5. Threshold - Inactive Consumer
```sql
-- File: 05_threshold_inactive_consumer.sql
-- Purpose: Detect consumer crashes (no offset commits > 60s)
```

### 6. Threshold - Rebalance Storm
```sql
-- File: 06_threshold_rebalance_storm.sql
-- Purpose: Detect frequent consumer rebalances
```

### 7. Correlation Engine
```sql
-- File: 07_correlation_engine.sql
-- Purpose: Detect correlated degradation (producer/broker issues)
```

## How to Deploy

### Option 1: Confluent Cloud UI (Recommended for First Time)

1. Navigate to **Confluent Cloud Console** → Your Environment → Flink
2. Create a Flink Compute Pool (if not exists):
   - Name: `kafka-sentinel-compute`
   - Region: Same as Kafka cluster
   - Max CFUs: 5 (for demo)
3. Open **Flink SQL Workspace**
4. Copy/paste each SQL file content **in order** (01 → 07)
5. Click **Run** for each statement
6. Monitor statement status in **Statements** tab

### Option 2: Confluent CLI

```bash
# Authenticate
confluent login

# Set environment and cluster context
confluent environment use <env-id>
confluent flink compute-pool use <pool-id>

# Submit statements
confluent flink statement create --sql "$(cat 01_formatting.sql)"
confluent flink statement create --sql "$(cat 02_arima_lag_trending_up.sql)"
# ... repeat for each file
```

### Option 3: Terraform (Future)

```hcl
# Coming soon: Terraform resource for Flink statements
# Track: https://github.com/confluentinc/terraform-provider-confluent
```

## Validation

After deploying all statements:

1. **Check Statement Status**:
   ```bash
   confluent flink statement list
   ```
   All should show `RUNNING` status.

2. **Verify Data Flow**:
   - Run velocity-monitor to publish metrics
   - Check `metrics_flattened` topic has data:
     ```bash
     confluent kafka topic consume metrics_flattened --from-beginning
     ```

3. **Trigger Test Alert**:
   - Use Data Simulator to inject `lag_spike` scenario
   - Wait ~30 seconds (ARIMA training window)
   - Check `velocity_anomaly_alerts` topic:
     ```bash
     confluent kafka topic consume velocity_anomaly_alerts --from-beginning
     ```

## Tuning Parameters

### ARIMA Model Parameters

Located in each `02_*.sql`, `03_*.sql`, `04_*.sql`:

- **`p`** (auto-regressive terms): 2-3 for most metrics
- **`d`** (differencing order): 1 for trending data
- **`q`** (moving average terms): 1-2 for smoothing
- **`seasonality`**: 12 (~2 min cycles at 10s poll interval)
- **`training_window`**: 80-100 data points (~13-16 minutes)
- **`threshold`**: 0.5-0.6 (anomaly sensitivity)

**To adjust**: Edit the `MAP[...]` in `ML_DETECT_ANOMALIES()` call.

### Threshold Rules

Located in `05_*.sql`, `06_*.sql`:

- **Inactive duration**: 60 seconds (in `05_threshold_inactive_consumer.sql`)
- **Rebalance frequency**: 3+ events in 3 minutes (in `06_threshold_rebalance_storm.sql`)
- **Correlation window**: 5 minutes (in `07_correlation_engine.sql`)

### Alert Severity Levels

Defined in each statement's `CASE` expression:

- **`critical`**: Anomaly score > 0.8 or always-critical events
- **`warning`**: Anomaly score 0.5-0.8
- **`info`**: Anomaly score < 0.5 (logged but not urgent)

## Troubleshooting

### "Table already exists" Error

If redeploying, drop existing tables first:
```sql
DROP TABLE IF EXISTS metrics_source;
DROP TABLE IF EXISTS metrics_flattened;
DROP TABLE IF EXISTS velocity_anomaly_alerts;
```

Then recreate via `01_formatting.sql`.

### "ML_DETECT_ANOMALIES not found" Error

- Verify your cluster is **Standard tier** or higher (Basic doesn't support ML functions)
- Check Flink Compute Pool has ML capabilities enabled

### No Alerts Generated

- Verify `metrics_source` has data (velocity-monitor running)
- ARIMA needs ~100 data points for training (~16 min at 10s polling)
- Check filter conditions (e.g., `current_lag > 1000` may exclude low-lag groups)
- Lower `threshold` in ARIMA config for more sensitive detection

### High Alert Volume (Too Noisy)

- Increase ARIMA `threshold` (e.g., 0.5 → 0.7)
- Tighten filter conditions (e.g., `current_lag > 5000` instead of 1000)
- Increase minimum deviation thresholds

## Cost Considerations

Flink pricing on Confluent Cloud:

- **Compute**: Charged per CFU-hour
- **Storage**: Charged per GB stored in state (ARIMA maintains model state)
- **Data Transfer**: Ingress/egress from Kafka topics

**Optimization tips**:
- Use shorter `training_window` (80 vs 100) to reduce state size
- Set appropriate `kafka.retention.time` on alert topics
- Pause unused statements when not demoing

## Next Steps

After deploying Flink pipeline:

1. **AI Agent** - Build Confluent Intelligence agent to enrich alerts
2. **Dashboard** - Visualize real-time alerts in React UI
3. **Feedback Loop** - Collect engineer feedback via `alert_feedback` topic
