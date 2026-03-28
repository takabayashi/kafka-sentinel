# Next Session Quick Start

## Current State (2026-03-27)

**✅ WORKING:**
- velocity-monitor publishing mock metrics every 10s
- metrics_flattened topic receiving messages (offset incrementing: 1, 2, 3...)
- Simulator generating events at 200 msg/s
- Slow consumer creating lag at 10 msg/s
- Flink table created and ready to consume

**❌ BLOCKED:**
- ARIMA anomaly detection jobs (ML_DETECT_ANOMALIES syntax unknown)
- velocity_anomaly_alerts topic (pending job deployment)

## Immediate Next Steps

### 1. Verify Flink Can Read Metrics (5 min)

Create SELECT query in Confluent Cloud Flink:

```sql
SELECT
  event_time,
  consumer_group,
  current_lag,
  read_speed,
  write_speed,
  is_falling_behind
FROM metrics_flattened
LIMIT 10;
```

**Expected:** Should see mock metrics with lag ~3000-3500, read_speed ~100-200 msg/s

### 2. Research ML_DETECT_ANOMALIES (CRITICAL)

**Option A:** Search Confluent documentation
- Confluent Cloud Flink ML Functions docs
- Example queries using ML_DETECT_ANOMALIES
- Return type definition (ROW schema)

**Option B:** Deploy threshold-based detector instead

```sql
INSERT INTO velocity_anomaly_alerts
SELECT
  UUID() AS alert_id,
  TO_TIMESTAMP(event_time) AS alert_time,
  'THRESHOLD_LAG_SPIKE' AS detection_type,
  'HIGH' AS severity,
  cluster_id,
  consumer_group,
  topic,
  current_lag,
  read_speed,
  write_speed,
  time_to_catchup_seconds,
  partition_skew_score,
  1.0 AS anomaly_score,
  CONCAT('Consumer lag exceeded 10K messages: ', CAST(current_lag AS STRING)) AS context
FROM metrics_flattened
WHERE current_lag > 10000;
```

### 3. Test End-to-End Alert Flow

Once alerts are generating:

1. Check velocity_anomaly_alerts topic in Confluent Cloud UI
2. Verify messages contain all required fields
3. Test with different anomaly conditions (lag spike, slow consumer, partition skew)

## Running Components

**Terminal 1 - Simulator:**
```bash
cd simulator
python main.py
# Visit http://localhost:5001/simulator/status
```

**Terminal 2 - Slow Consumer (creates lag):**
```bash
cd simulator
python slow_consumer.py
```

**Terminal 3 - Velocity Monitor:**
```bash
cd velocity-monitor
python main.py
```

## Key Files to Review

- `WORKAROUNDS.md` - All known issues and solutions
- `velocity-monitor/consumer_group_api.py:63-113` - Mock lag generator
- `flink/jobs-arima/01_arima_lag_trending_up.sql` - Blocked ARIMA job
- `flink/catalog/03_create_velocity_anomaly_alerts.sql` - Sink table schema

## Environment

- Cluster: STANDARD tier (lkc-...)
- Topics: 7 topics created via Terraform
- Schema Registry: Recreated (fresh schemas)
- Consumer group: test-consumer (STABLE state)

## Decision Points

**If ML_DETECT_ANOMALIES syntax found:**
- Deploy ARIMA jobs as originally planned
- Test with simulator scenarios (lag_spike, consumer_slow)

**If ML_DETECT_ANOMALIES blocked:**
- Deploy threshold-based detector
- Still achieves demo goal (real-time anomaly detection)
- Can upgrade to ML later on DEDICATED cluster

## Success Criteria

End-to-end pipeline validated when:
1. ✅ Metrics flowing to metrics_flattened
2. ⏳ Flink SELECT query returns data
3. ⏳ Alerts generating in velocity_anomaly_alerts
4. ⏳ AI agent enriches alerts (future phase)
5. ⏳ Dashboard displays real-time alerts (future phase)

**Current Progress: 1/5 complete**
