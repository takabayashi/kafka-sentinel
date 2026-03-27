CREATE TABLE IF NOT EXISTS velocity_anomaly_alerts (
  `alert_id` STRING,
  `alert_time` TIMESTAMP(3),
  `detection_type` STRING,
  `severity` STRING,
  `cluster_id` STRING,
  `consumer_group` STRING,
  `topic` STRING,
  `current_lag` BIGINT,
  `read_speed` DOUBLE,
  `write_speed` DOUBLE,
  `time_to_catchup_seconds` DOUBLE,
  `partition_skew_score` DOUBLE,
  `anomaly_score` DOUBLE,
  `context` STRING,
  WATERMARK FOR `alert_time` AS `alert_time` - INTERVAL '10' SECONDS
) WITH (
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.retention.time' = '7 d',
  'value.format' = 'json-registry'
);
