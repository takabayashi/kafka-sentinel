-- ================================================================
-- Flink SQL Statement 03: ARIMA - Speed Trending Down
-- ================================================================
-- Purpose: Detect when consumer read speed is trending downward
--          using ARIMA. This catches performance degradation in
--          consumer processing throughput.
--
-- Input:  metrics_flattened
-- Output: velocity_anomaly_alerts
--
-- Requires: Confluent Cloud Standard cluster (ML_DETECT_ANOMALIES)
-- ================================================================

INSERT INTO velocity_anomaly_alerts
SELECT
  CONCAT(
    'arima_speed_down_',
    consumer_group, '_',
    CAST(UNIX_TIMESTAMP(CAST(event_time AS TIMESTAMP(3))) AS STRING)
  ) AS alert_id,
  event_time AS alert_time,
  'arima_speed_down' AS detection_type,
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
  JSON_OBJECT(
    'predicted_speed' VALUE anomaly_data.prediction,
    'actual_speed' VALUE read_speed,
    'deviation_percent' VALUE ROUND(
      ABS(read_speed - anomaly_data.prediction) / NULLIF(anomaly_data.prediction, 0) * 100,
      2
    ),
    'speed_ratio' VALUE speed_ratio,
    'is_falling_behind' VALUE is_falling_behind,
    'model_type' VALUE 'ARIMA'
  ) AS context
FROM (
  SELECT
    *,
    ML_DETECT_ANOMALIES(
      read_speed,
      MAP[
        'algorithm', 'ARIMA',
        'p', '3',
        'd', '1',
        'q', '2',
        'seasonality', '12',
        'threshold', '0.5',
        'training_window', '100'
      ]
    ) OVER (
      PARTITION BY consumer_group
      ORDER BY event_time
      ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
    ) AS anomaly_data
  FROM metrics_flattened
  WHERE read_speed > 0  -- Only analyze active consumers
)
WHERE
  anomaly_data.is_anomaly = TRUE
  -- Filter: Only when speed is actually decreasing
  AND read_speed < anomaly_data.prediction
  -- Filter: Significant drop (more than 20% below prediction)
  AND (anomaly_data.prediction - read_speed) / NULLIF(anomaly_data.prediction, 0) > 0.2;
