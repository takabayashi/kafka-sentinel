-- ================================================================
-- Flink SQL Job 01: Formatting - Compute Derived Metrics
-- ================================================================
-- Purpose: Transform raw metrics from metrics_source into enriched
--          metrics in metrics_flattened by computing:
--          - lag_velocity (rate of lag change)
--          - speed_ratio (read_speed / write_speed)
--          - is_falling_behind (boolean indicator)
--
-- Input:  metrics_source (raw metrics from velocity-monitor)
-- Output: metrics_flattened (enriched metrics for ARIMA rules)
-- ================================================================

INSERT INTO metrics_flattened
SELECT
  `timestamp` AS event_time,
  cluster_id,
  consumer_group,
  topic,
  current_lag,
  read_speed,
  write_speed,
  time_to_catchup_seconds,
  partition_skew_score,
  lag_24h_avg,
  lag_percentile_95_24h,
  partition_count,
  -- Compute lag velocity (change in lag over time)
  CAST(
    current_lag - COALESCE(
      LAG(current_lag, 1) OVER (
        PARTITION BY consumer_group
        ORDER BY TO_TIMESTAMP(`timestamp`)
      ),
      current_lag
    ) AS DOUBLE
  ) AS lag_velocity,
  -- Compute speed ratio
  CASE
    WHEN write_speed > 0 THEN read_speed / write_speed
    ELSE 1.0
  END AS speed_ratio,
  -- Is falling behind indicator
  read_speed < write_speed AS is_falling_behind
FROM metrics_source
WHERE current_lag >= 0;  -- Filter out invalid data
