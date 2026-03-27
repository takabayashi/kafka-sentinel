-- ================================================================
-- Flink SQL Statement 05: Threshold - Consumer Group Inactive
-- ================================================================
-- Purpose: Detect when a consumer group stops consuming (no offset
--          commits for extended period). This catches crashed
--          consumers, deployment issues, or network partitions.
--
-- Input:  metrics_flattened
-- Output: velocity_anomaly_alerts
--
-- Detection Logic: No change in consumed offset for > 60 seconds
-- ================================================================

INSERT INTO velocity_anomaly_alerts
SELECT
  CONCAT(
    'threshold_inactive_',
    consumer_group, '_',
    CAST(UNIX_TIMESTAMP(CAST(event_time AS TIMESTAMP(3))) AS STRING)
  ) AS alert_id,
  event_time AS alert_time,
  'threshold_inactive' AS detection_type,
  'critical' AS severity,  -- Always critical (consumer is down)
  cluster_id,
  consumer_group,
  topic,
  current_lag,
  read_speed,
  write_speed,
  time_to_catchup_seconds,
  partition_skew_score,
  1.0 AS anomaly_score,  -- Threshold rules use score 1.0
  JSON_OBJECT(
    'inactive_duration_seconds' VALUE inactive_duration_seconds,
    'last_active_time' VALUE CAST(last_active_time AS STRING),
    'lag_growth' VALUE current_lag - COALESCE(first_lag_in_window, current_lag),
    'recommendation' VALUE 'Check consumer group health, deployments, and network connectivity'
  ) AS context
FROM (
  SELECT
    *,
    -- Time since consumer was last active (read_speed > 0)
    TIMESTAMPDIFF(
      SECOND,
      LAST_VALUE(event_time) FILTER (WHERE read_speed > 0) OVER w,
      event_time
    ) AS inactive_duration_seconds,
    LAST_VALUE(event_time) FILTER (WHERE read_speed > 0) OVER w AS last_active_time,
    FIRST_VALUE(current_lag) OVER w AS first_lag_in_window
  FROM metrics_flattened
  WINDOW w AS (
    PARTITION BY consumer_group
    ORDER BY event_time
    RANGE BETWEEN INTERVAL '2' MINUTE PRECEDING AND CURRENT ROW
  )
)
WHERE
  -- Consumer has been inactive for more than 60 seconds
  inactive_duration_seconds > 60
  -- But was active in the last 2 minutes (avoid re-alerting on dead groups)
  AND inactive_duration_seconds < 120
  -- There is lag (producer is still writing)
  AND current_lag > 0;
