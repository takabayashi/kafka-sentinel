-- ================================================================
-- Flink SQL Statement 02: ARIMA - Lag Trending Up
-- ================================================================
-- Purpose: Detect when consumer lag is trending upward using ARIMA
--          time-series forecasting. This catches slow degradation
--          that threshold-based alerts miss.
--
-- Input:  metrics_flattened
-- Output: velocity_anomaly_alerts
--
-- Requires: Confluent Cloud Standard cluster (ML_DETECT_ANOMALIES)
-- ================================================================

-- Alert Sink Table (shared by all detection rules)
CREATE TABLE IF NOT EXISTS velocity_anomaly_alerts (
  `alert_id` STRING,
  `alert_time` TIMESTAMP(3),
  `detection_type` STRING,  -- 'arima_lag_up', 'arima_speed_down', 'threshold_inactive', etc.
  `severity` STRING,         -- 'critical', 'warning', 'info'
  `cluster_id` STRING,
  `consumer_group` STRING,
  `topic` STRING,
  `current_lag` BIGINT,
  `read_speed` DOUBLE,
  `write_speed` DOUBLE,
  `time_to_catchup_seconds` DOUBLE,
  `partition_skew_score` DOUBLE,
  `anomaly_score` DOUBLE,   -- ML confidence score (0-1)
  `context` STRING,          -- JSON with additional context
  WATERMARK FOR `alert_time` AS `alert_time` - INTERVAL '10' SECONDS,
  PRIMARY KEY (`alert_id`) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.retention.time' = '7 d',
  'value.format' = 'json-registry',
  'value.fields-include' = 'EXCEPT_KEY',
  'key.format' = 'raw',
  'key.fields' = 'alert_id'
);

-- ARIMA Detection: Lag Trending Up
INSERT INTO velocity_anomaly_alerts
SELECT
  -- Generate unique alert ID
  CONCAT(
    'arima_lag_up_',
    consumer_group, '_',
    CAST(UNIX_TIMESTAMP(CAST(event_time AS TIMESTAMP(3))) AS STRING)
  ) AS alert_id,
  event_time AS alert_time,
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
    *,
    ML_DETECT_ANOMALIES(
      current_lag,
      -- ARIMA configuration
      MAP[
        'algorithm', 'ARIMA',
        'p', '3',              -- Auto-regressive terms
        'd', '1',              -- Differencing order
        'q', '2',              -- Moving average terms
        'seasonality', '12',   -- Seasonal period (~2 min at 10s intervals)
        'threshold', '0.5',    -- Anomaly threshold
        'training_window', '100'  -- Last 100 data points (~16 minutes)
      ]
    ) OVER (
      PARTITION BY consumer_group
      ORDER BY event_time
      ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
    ) AS anomaly_data
  FROM metrics_flattened
  WHERE current_lag > 0  -- Only analyze groups with lag
)
WHERE
  -- Filter: Only emit alerts when anomaly detected
  anomaly_data.is_anomaly = TRUE
  -- Filter: Only when lag is actually increasing
  AND lag_velocity > 0
  -- Filter: Minimum lag threshold (avoid noise on low-lag groups)
  AND current_lag > 1000;
