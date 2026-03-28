-- ================================================================
-- Flink SQL Job: ARIMA - Lag Trending Up
-- ================================================================
-- Detects when consumer lag is trending upward using ARIMA forecasting
--
-- Input:  metrics_flattened (enriched metrics from velocity-monitor)
-- Output: velocity_anomaly_alerts
-- ================================================================

INSERT INTO velocity_anomaly_alerts
SELECT
  -- Generate unique alert ID
  CONCAT(
    'arima_lag_up_',
    consumer_group, '_',
    CAST(UNIX_TIMESTAMP(event_time) AS STRING)
  ) AS alert_id,
  TO_TIMESTAMP(event_time) AS alert_time,
  'arima_lag_up' AS detection_type,
  -- Severity based on anomaly score
  CASE
    WHEN anomaly_data.anomaly_score > 0.8 THEN 'critical'
    WHEN anomaly_data.anomaly_score > 0.5 THEN 'warning'
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
  anomaly_data.anomaly_score,
  -- Context JSON with ARIMA predictions
  JSON_OBJECT(
    'predicted_lag' VALUE anomaly_data.prediction,
    'actual_lag' VALUE current_lag,
    'deviation_percent' VALUE ROUND(
      ABS(current_lag - anomaly_data.prediction) / NULLIF(anomaly_data.prediction, 0) * 100,
      2
    ),
    'lag_velocity' VALUE lag_velocity,
    'window_size_minutes' VALUE 10,
    'model_type' VALUE 'ARIMA'
  ) AS context
FROM (
  SELECT
    event_time,
    cluster_id,
    consumer_group,
    topic,
    current_lag,
    read_speed,
    write_speed,
    time_to_catchup_seconds,
    partition_skew_score,
    lag_velocity,
    ML_DETECT_ANOMALIES(
      CAST(current_lag AS DOUBLE),
      TO_TIMESTAMP(event_time),
      '{"algorithm":"ARIMA","p":3,"d":1,"q":2,"seasonality":12,"threshold":0.5,"training_window":100}'
    ) OVER (
      PARTITION BY consumer_group
      ORDER BY TO_TIMESTAMP(event_time)
      ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
    ) AS anomaly_data
  FROM metrics_flattened
  WHERE current_lag > 1000
    AND lag_velocity > 0
)
WHERE anomaly_data.is_anomaly = TRUE;
