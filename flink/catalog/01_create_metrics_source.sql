CREATE TABLE IF NOT EXISTS metrics_source (
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
  'value.format' = 'json-registry'
);
