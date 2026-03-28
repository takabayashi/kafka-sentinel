-- ================================================================
-- Flink SQL Job: ARIMA - Lag Trending Up (Simplified)
-- ================================================================
-- Detects when consumer lag is trending upward using thresholds
-- NOTE: Using thresholds instead of ML_DETECT_ANOMALIES for now
--       due to ROW field access limitations in Confluent Cloud Flink
--
-- Input:  metrics_flattened (enriched metrics from velocity-monitor)
-- Output: velocity_anomaly_alerts
-- ================================================================

INSERT INTO velocity_anomaly_alerts
SELECT
  CONCAT('arima_lag_up_', consumer_group, '_', CAST(UNIX_TIMESTAMP(event_time) AS STRING)) AS alert_id,
  TO_TIMESTAMP(event_time) AS alert_time,
  'arima_lag_up' AS detection_type,
  CASE
    WHEN current_lag > lag_24h_avg * 3 THEN 'critical'
    WHEN current_lag > lag_24h_avg * 2 THEN 'warning'
    ELSE 'info'
  END AS severity,
  cluster_id,
  consumer_group,
  topic,
  current_lag,
  read_speed,
  write_speed,
  time_to_catchup_seconds,
  partition_skew_score,
  -- Compute simple anomaly score based on lag deviation from 24h average
  LEAST(1.0, CAST(current_lag AS DOUBLE) / NULLIF(lag_24h_avg, 0) / 5.0) AS anomaly_score,
  JSON_OBJECT(
    'lag_vs_avg_ratio' VALUE ROUND(CAST(current_lag AS DOUBLE) / NULLIF(lag_24h_avg, 0), 2),
    'actual_lag' VALUE current_lag,
    'lag_24h_avg' VALUE lag_24h_avg,
    'lag_velocity' VALUE lag_velocity,
    'window_size_minutes' VALUE 10,
    'model_type' VALUE 'THRESHOLD'
  ) AS context
FROM metrics_flattened
WHERE current_lag > 1000
  AND lag_velocity > 0
  AND current_lag > lag_24h_avg * 1.5;
