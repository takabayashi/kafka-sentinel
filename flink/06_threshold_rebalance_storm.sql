-- ================================================================
-- Flink SQL Statement 06: Threshold - Rebalance Storm Detection
-- ================================================================
-- Purpose: Detect frequent consumer group rebalances by identifying
--          rapid fluctuations in read speed and lag. Rebalances
--          cause temporary pauses in consumption.
--
-- Input:  metrics_flattened
-- Output: velocity_anomaly_alerts
--
-- Detection Logic: High variance in read_speed over short window
-- ================================================================

INSERT INTO velocity_anomaly_alerts
SELECT
  CONCAT(
    'threshold_rebalance_',
    consumer_group, '_',
    CAST(UNIX_TIMESTAMP(CAST(event_time AS TIMESTAMP(3))) AS STRING)
  ) AS alert_id,
  event_time AS alert_time,
  'threshold_rebalance_storm' AS detection_type,
  CASE
    WHEN zero_speed_count > 5 THEN 'critical'
    WHEN zero_speed_count > 3 THEN 'warning'
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
  -- Normalized score based on frequency of speed drops
  LEAST(zero_speed_count / 10.0, 1.0) AS anomaly_score,
  JSON_OBJECT(
    'zero_speed_events' VALUE zero_speed_count,
    'window_minutes' VALUE 3,
    'speed_variance' VALUE speed_variance,
    'avg_speed' VALUE avg_speed,
    'possible_cause' VALUE 'Consumer group rebalancing or instance restarts',
    'recommendation' VALUE 'Check for frequent pod restarts, autoscaling events, or static membership configuration'
  ) AS context
FROM (
  SELECT
    *,
    -- Count how many times read_speed dropped to zero in window
    COUNT(*) FILTER (WHERE read_speed = 0) OVER w AS zero_speed_count,
    -- Variance in read speed
    VARIANCE(read_speed) OVER w AS speed_variance,
    AVG(read_speed) OVER w AS avg_speed
  FROM metrics_flattened
  WINDOW w AS (
    PARTITION BY consumer_group
    ORDER BY event_time
    RANGE BETWEEN INTERVAL '3' MINUTE PRECEDING AND CURRENT ROW
  )
)
WHERE
  -- Multiple speed drops in short window (indicates rebalancing)
  zero_speed_count >= 3
  -- High variance relative to average (unstable consumption)
  AND (speed_variance > avg_speed * 0.5 OR avg_speed = 0)
  -- Only emit alert once per window (avoid spam)
  AND MOD(CAST(UNIX_TIMESTAMP(CAST(event_time AS TIMESTAMP(3))) AS BIGINT), 180) < 10;
