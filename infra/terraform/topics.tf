# Topic 1: Simulator Events
# Synthetic events from the data simulator (free producer + scenario buttons)
resource "confluent_kafka_topic" "simulator_events" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.main.id
  }
  rest_endpoint = data.confluent_kafka_cluster.main.rest_endpoint

  topic_name       = "simulator_events"
  partitions_count = 3

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = var.topic_retention_ms
  }

  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}

# Topic 2: Metrics Source
# Raw metrics from Velocity Monitor (Consumer Group API polling every ~10s)
resource "confluent_kafka_topic" "metrics_source" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.main.id
  }
  rest_endpoint = data.confluent_kafka_cluster.main.rest_endpoint

  topic_name       = "metrics_source"
  partitions_count = 6

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = var.topic_retention_ms
  }

  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}

# Topic 3: Metrics Flattened
# Normalized metrics after Flink formatting step (input for anomaly detection)
resource "confluent_kafka_topic" "metrics_flattened" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.main.id
  }
  rest_endpoint = data.confluent_kafka_cluster.main.rest_endpoint

  topic_name       = "metrics_flattened"
  partitions_count = 6

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = var.topic_retention_ms
  }

  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}

# Topic 4: Velocity Anomaly Alerts
# Alerts from all Flink anomaly detection rules (ARIMA + thresholds + correlation)
resource "confluent_kafka_topic" "velocity_anomaly_alerts" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.main.id
  }
  rest_endpoint = data.confluent_kafka_cluster.main.rest_endpoint

  topic_name       = "velocity_anomaly_alerts"
  partitions_count = 3

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = var.topic_retention_ms
  }

  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}

# Topic 5: Enriched Alerts
# Alerts enriched by AI Agent with diagnosis, severity, and recommended actions
resource "confluent_kafka_topic" "enriched_alerts" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.main.id
  }
  rest_endpoint = data.confluent_kafka_cluster.main.rest_endpoint

  topic_name       = "enriched_alerts"
  partitions_count = 3

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = var.topic_retention_ms
  }

  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}

# Topic 6: Agent Memory (COMPACTED)
# Historical context for AI Agent - maintains latest state per consumer group
# This is the ONLY compacted topic in the system
resource "confluent_kafka_topic" "agent_memory" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.main.id
  }
  rest_endpoint = data.confluent_kafka_cluster.main.rest_endpoint

  topic_name       = "agent_memory"
  partitions_count = 6

  config = {
    "cleanup.policy"        = "compact"
    "min.compaction.lag.ms" = "60000"      # 1 minute
    "delete.retention.ms"   = "86400000"   # 24 hours for tombstones
    "segment.ms"            = "3600000"    # 1 hour segments
    "max.compaction.lag.ms" = "604800000"  # 7 days (minimum allowed)
  }

  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}

# Topic 7: Alert Feedback
# Thumbs up/down feedback from Dashboard UI for future fine-tuning
resource "confluent_kafka_topic" "alert_feedback" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.main.id
  }
  rest_endpoint = data.confluent_kafka_cluster.main.rest_endpoint

  topic_name       = "alert_feedback"
  partitions_count = 1

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = var.topic_retention_ms
  }

  credentials {
    key    = var.kafka_api_key
    secret = var.kafka_api_secret
  }
}
