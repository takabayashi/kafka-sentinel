# Workarounds and Known Issues

## STANDARD Cluster Limitation: Consumer Group Lag API

**Issue:** Confluent Cloud STANDARD tier clusters do not support the Consumer Group Lag API endpoints:
- `/kafka/v3/clusters/{cluster_id}/consumer-groups/{group_id}/lag`
- `/kafka/v3/clusters/{cluster_id}/consumer-groups/{group_id}/lag-summary`

Both return HTTP 404. This API is only available on DEDICATED clusters ($thousands/month vs $1/hour STANDARD).

**Impact:** velocity-monitor cannot fetch real consumer lag data from the cluster.

**Workaround Implemented:** Mock lag data generator in `consumer_group_api.py`

```python
def get_consumer_group_lag(self, group_id: str) -> Optional[List[Dict]]:
    # WORKAROUND: STANDARD clusters don't support lag API
    # Generate mock lag data for testing downstream pipeline
    logger.info(f"Using MOCK lag data for {group_id} (STANDARD cluster limitation)")

    import random
    import time

    if group_id == "test-consumer":
        base_lag = int(time.time() % 10000)
        mock_lag = [
            {
                "cluster_id": self.cluster_id,
                "consumer_group_id": group_id,
                "topic_name": "simulator_events",
                "partition_id": 0,
                "current_offset": 5000 + base_lag,
                "log_end_offset": 8000 + base_lag + random.randint(100, 500),
                "lag": 3000 + random.randint(100, 500),
                # ... additional fields
            },
            # ... partitions 1 and 2
        ]
        return mock_lag
    return None
```

**Production Solution:** Upgrade to DEDICATED cluster or use Confluent Metrics API (has multi-minute delay).

---

## ML_DETECT_ANOMALIES Field Access Syntax

**Issue:** Unknown how to access fields from ML_DETECT_ANOMALIES return value.

**Attempted Syntax:**
```sql
-- Failed: Unknown field 'anomaly_score'
(ML_DETECT_ANOMALIES(...)).anomaly_score

-- Failed: Field not found in subquery
SELECT anomaly_data.anomaly_score
FROM (SELECT ML_DETECT_ANOMALIES(...) AS anomaly_data FROM ...)
```

**Expected Return Type:** ROW with fields like `anomaly_score`, `is_anomaly`, etc.

**Blocking:** Cannot deploy ARIMA anomaly detection jobs until syntax is resolved.

**Next Steps:**
1. Search Confluent documentation for ML_DETECT_ANOMALIES examples
2. Test on DEDICATED cluster with full ML function support
3. Consider alternative: Deploy threshold-based anomaly detector instead

---

## Schema Registry Recreation

**Issue:** Cached incompatible schemas from previous attempts caused persistent validation errors.

**Solution:** Delete schema (soft + permanent delete), then recreate table with fresh deployment.

**Commands:**
```bash
# Via Confluent Cloud UI: Topics → metrics_flattened → Schema → Delete Schema
# Then: Delete Permanently
# Then: Re-run CREATE TABLE statement
```

This resolved all "Column types of query result and sink do not match" errors.

---

## Simulator Scenario Bug

**Issue:** Scenarios failed with `TypeError: run() got multiple values for keyword argument 'consumer_group'`

**Cause:** Passing consumer_group both explicitly and in **data dict spread.

**Fix:** Changed `data.get()` to `data.pop()` to remove from dict before spreading:

```python
# Get scenario parameters from request
data = request.json or {}
consumer_group = data.pop('consumer_group', 'checkout-service')  # Changed from get to pop
target_topic = data.pop('target_topic', 'orders')

scenario.run(
    consumer_group=consumer_group,
    target_topic=target_topic,
    **data  # Now consumer_group is not in data
)
```

---

## Testing Setup

**Components Working:**
- ✅ Simulator free producer (200 msg/s)
- ✅ Slow consumer (10 msg/s, creates lag)
- ✅ velocity-monitor with mock data (publishing every 10s)
- ✅ metrics_flattened topic (messages flowing, offsets incrementing)
- ✅ Flink table created and ready

**Blocked:**
- ❌ ARIMA jobs (ML_DETECT_ANOMALIES syntax unknown)
- ⏸️ velocity_anomaly_alerts topic (pending job deployment)

**End-to-End Validation:**
```
[velocity-monitor with mock data]
      ↓
[metrics_flattened topic] ✅ WORKING
      ↓
[Flink table] ✅ READY
      ↓
[ARIMA job] ❌ BLOCKED on ML syntax
      ↓
[velocity_anomaly_alerts topic] ⏸️ PENDING
```

---

## Next Session TODO

1. **Immediate:** Create SELECT query to verify Flink can read from metrics_flattened
2. **Research:** Find ML_DETECT_ANOMALIES documentation or test on DEDICATED cluster
3. **Alternative:** Deploy simple threshold-based anomaly detector as fallback
4. **Optimization:** Replace mock data with real metrics once on DEDICATED cluster
