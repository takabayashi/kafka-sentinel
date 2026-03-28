CREATE TABLE metrics_flattened (
  `event_time` STRING,
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
  `lag_velocity` DOUBLE,
  `speed_ratio` DOUBLE,
  `is_falling_behind` BOOLEAN
) WITH (
  'connector' = 'confluent',
  'value.format' = 'json-registry'
);
