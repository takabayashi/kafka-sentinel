-- ================================================================
-- Flink SQL Statement 04: ARIMA - Hot Partition Prediction
-- ================================================================
-- Purpose: Detect partition skew anomalies using ARIMA. High skew
--          indicates uneven load distribution (hot partitions),
--          which degrades consumer parallelism.
--
-- Input:  metrics_flattened
-- Output: velocity_anomaly_alerts
--
-- Requires: Confluent Cloud Standard cluster (ML_DETECT_ANOMALIES)
-- ================================================================

INSERT INTO velocity_anomaly_alerts
SELECT
  CONCAT(
    'arima_hot_partition_',
    consumer_group, '_',
    CAST(UNIX_TIMESTAMP(CAST(event_time AS TIMESTAMP(3))) AS STRING)
  ) AS alert_id,
  event_time AS alert_time,
  'arima_hot_partition' AS detection_type,
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
    'predicted_skew' VALUE anomaly_data.prediction,
    'actual_skew' VALUE partition_skew_score,
    'deviation_percent' VALUE ROUND(
      ABS(partition_skew_score - anomaly_data.prediction) / NULLIF(anomaly_data.prediction, 0) * 100,
      2
    ),
    'partition_count' VALUE partition_count,
    'recommendation' VALUE 'Review partition key distribution or scale consumer instances',
    'model_type' VALUE 'ARIMA'
  ) AS context
FROM (
  SELECT
    *,
    ML_DETECT_ANOMALIES(
      partition_skew_score,
      MAP[
        'algorithm', 'ARIMA',
        'p', '2',              -- Less complex model for skew
        'd', '1',
        'q', '1',
        'seasonality', '6',    -- Shorter seasonal period
        'threshold', '0.6',    -- Higher threshold (skew naturally varies)
        'training_window', '80'
      ]
    ) OVER (
      PARTITION BY consumer_group
      ORDER BY event_time
      ROWS BETWEEN 80 PRECEDING AND CURRENT ROW
    ) AS anomaly_data
  FROM metrics_flattened
  WHERE
    partition_count > 1  -- Only multi-partition topics
    AND partition_skew_score > 0
)
WHERE
  anomaly_data.is_anomaly = TRUE
  -- Filter: Skew significantly higher than predicted
  AND partition_skew_score > anomaly_data.prediction
  -- Filter: Absolute skew threshold (> 2.0 means max lag is 2x average)
  AND partition_skew_score > 2.0;
