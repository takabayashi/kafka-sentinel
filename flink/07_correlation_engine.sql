-- ================================================================
-- Flink SQL Statement 07: Correlation Engine
-- ================================================================
-- Purpose: Detect correlated degradation across multiple consumer
--          groups on the same topic. This distinguishes:
--          - Consumer issues (single group affected)
--          - Producer/broker issues (all groups affected)
--
-- Input:  metrics_flattened
-- Output: velocity_anomaly_alerts
--
-- Detection Logic: 2+ groups on same topic showing degradation
--                  within 5-minute window
-- ================================================================

INSERT INTO velocity_anomaly_alerts
SELECT
  CONCAT(
    'correlation_',
    topic, '_',
    CAST(UNIX_TIMESTAMP(CAST(event_time AS TIMESTAMP(3))) AS STRING)
  ) AS alert_id,
  event_time AS alert_time,
  'correlation_multi_group' AS detection_type,
  CASE
    WHEN affected_group_count >= 3 THEN 'critical'
    WHEN affected_group_count = 2 THEN 'warning'
    ELSE 'info'
  END AS severity,
  cluster_id,
  ANY_VALUE(consumer_group) AS consumer_group,  -- Representative group
  topic,
  SUM(current_lag) AS current_lag,  -- Total lag across all groups
  AVG(read_speed) AS read_speed,
  AVG(write_speed) AS write_speed,
  MAX(time_to_catchup_seconds) AS time_to_catchup_seconds,
  AVG(partition_skew_score) AS partition_skew_score,
  -- Score based on how many groups are affected
  LEAST(affected_group_count / 5.0, 1.0) AS anomaly_score,
  JSON_OBJECT(
    'affected_groups' VALUE affected_group_count,
    'total_groups_on_topic' VALUE total_group_count,
    'degrading_groups' VALUE LISTAGG(DISTINCT consumer_group, ', '),
    'root_cause' VALUE CASE
      WHEN affected_group_count >= total_group_count * 0.8
        THEN 'Likely producer or broker issue (most groups affected)'
      ELSE 'Mixed issue - check both producer and consumer health'
    END,
    'recommendation' VALUE 'Investigate topic write throughput, broker metrics, and producer health'
  ) AS context
FROM (
  SELECT
    *,
    -- Count degrading groups per topic in 5-minute window
    COUNT(DISTINCT consumer_group) FILTER (
      WHERE is_falling_behind = TRUE
        OR (lag_velocity > 100)  -- Lag increasing rapidly
        OR (read_speed < lag_24h_avg * 0.5)  -- Speed dropped below historical
    ) OVER topic_window AS affected_group_count,
    -- Total groups consuming from this topic
    COUNT(DISTINCT consumer_group) OVER topic_window AS total_group_count
  FROM metrics_flattened
  WINDOW topic_window AS (
    PARTITION BY topic
    ORDER BY event_time
    RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
  )
)
WHERE
  -- Multiple groups are degrading simultaneously
  affected_group_count >= 2
  -- At least one group is currently showing issues
  AND (is_falling_behind = TRUE OR lag_velocity > 100)
GROUP BY
  event_time,
  cluster_id,
  topic,
  affected_group_count,
  total_group_count
-- Deduplicate: emit one alert per topic per 5-minute window
HAVING
  MOD(CAST(UNIX_TIMESTAMP(CAST(MIN(event_time) AS TIMESTAMP(3))) AS BIGINT), 300) < 10;
