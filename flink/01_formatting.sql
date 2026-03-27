-- ================================================================
-- Flink SQL Statement 01: Formatting & Normalization
-- ================================================================
-- Purpose: Read raw metrics from velocity-monitor and flatten into
--          a normalized format for downstream anomaly detection.
--
-- Input:  metrics_source (raw JSON from velocity-monitor)
-- Output: metrics_flattened (normalized table format)
--
-- This statement must run FIRST before any anomaly detection rules.
-- ================================================================

-- Source Table: Read raw metrics from Kafka
CREATE TABLE metrics_source (
  `timestamp` STRING,
  `cluster_id` STRING,
  `consumer_group` STRING,
  `topic` STRING,
  `current_lag` BIGINT,
  `read_speed` DOUBLE,
  `write_speed` DOUBLE,
  `time_to_catchup_seconds` DOUBLE,
  `partition_skew_score` DOUBLE,
  `lag_24h_avg` DOUBLE,
  `lag_percentile_95_24h` DOUBLE,
  `partition_count` INT,
  `partitions` ARRAY<ROW<
    `partition_id` INT,
    `topic` STRING,
    `lag` BIGINT,
    `consumed_offset` BIGINT,
    `log_end_offset` BIGINT
  >>,
  `event_time` AS TO_TIMESTAMP(`timestamp`),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '30' SECONDS
) WITH (
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.retention.time' = '7 d',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'json-registry',
  'value.fields-include' = 'ALL',
  'key.format' = 'raw',
  'key.fields' = 'consumer_group'
);

-- Sink Table: Write normalized metrics to Kafka
CREATE TABLE metrics_flattened (
  `event_time` TIMESTAMP(3),
  `cluster_id` STRING,
  `consumer_group` STRING,
  `topic` STRING,
  `current_lag` BIGINT,
  `read_speed` DOUBLE,
  `write_speed` DOUBLE,
  `time_to_catchup_seconds` DOUBLE,
  `partition_skew_score` DOUBLE,
  `lag_24h_avg` DOUBLE,
  `lag_percentile_95_24h` DOUBLE,
  `partition_count` INT,
  -- Computed metrics for anomaly detection
  `lag_velocity` DOUBLE,  -- Rate of lag change
  `speed_ratio` DOUBLE,   -- read_speed / write_speed
  `is_falling_behind` BOOLEAN,  -- read_speed < write_speed
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '30' SECONDS,
  PRIMARY KEY (`consumer_group`, `event_time`) NOT ENFORCED
) WITH (
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.retention.time' = '7 d',
  'value.format' = 'json-registry',
  'value.fields-include' = 'EXCEPT_KEY',
  'key.format' = 'raw',
  'key.fields' = 'consumer_group'
);

-- Transformation: Flatten and enrich metrics
INSERT INTO metrics_flattened
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
  lag_24h_avg,
  lag_percentile_95_24h,
  partition_count,
  -- Compute lag velocity (rate of change)
  -- This will be used by ARIMA to detect trending
  COALESCE(
    (current_lag - LAG(current_lag, 1) OVER w) /
    EXTRACT(EPOCH FROM (event_time - LAG(event_time, 1) OVER w)),
    0.0
  ) AS lag_velocity,
  -- Speed ratio (how fast consumer is vs producer)
  CASE
    WHEN write_speed > 0 THEN read_speed / write_speed
    ELSE 1.0
  END AS speed_ratio,
  -- Boolean flag for falling behind
  read_speed < write_speed AS is_falling_behind
FROM metrics_source
WINDOW w AS (
  PARTITION BY consumer_group
  ORDER BY event_time
  ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
);
