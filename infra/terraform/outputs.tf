output "kafka_bootstrap_endpoint" {
  description = "Kafka bootstrap endpoint for producer/consumer connections"
  value       = data.confluent_kafka_cluster.main.bootstrap_endpoint
}

output "kafka_rest_endpoint" {
  description = "Kafka REST endpoint for REST Proxy access"
  value       = data.confluent_kafka_cluster.main.rest_endpoint
}

output "environment_id" {
  description = "Confluent Cloud Environment ID"
  value       = var.environment_id
}

output "cluster_id" {
  description = "Confluent Cloud Kafka Cluster ID"
  value       = var.cluster_id
}

output "topic_names" {
  description = "Map of all Kafka topic names for application integration"
  value = {
    simulator_events         = confluent_kafka_topic.simulator_events.topic_name
    metrics_source          = confluent_kafka_topic.metrics_source.topic_name
    metrics_flattened       = confluent_kafka_topic.metrics_flattened.topic_name
    velocity_anomaly_alerts = confluent_kafka_topic.velocity_anomaly_alerts.topic_name
    enriched_alerts         = confluent_kafka_topic.enriched_alerts.topic_name
    agent_memory            = confluent_kafka_topic.agent_memory.topic_name
    alert_feedback          = confluent_kafka_topic.alert_feedback.topic_name
  }
}

output "flink_service_account_id" {
  description = "Flink Service Account ID"
  value       = confluent_service_account.flink.id
}

output "flink_compute_pool_id" {
  description = "Flink Compute Pool ID (use with deploy-flink-statements.sh)"
  value       = confluent_flink_compute_pool.main.id
}

output "flink_rest_endpoint" {
  description = "Flink REST API endpoint"
  value       = local.flink_rest_endpoint
}

output "flink_catalog_tables" {
  description = "Flink catalog table statement names"
  value = {
    metrics_source            = confluent_flink_statement.create_metrics_source.statement_name
    metrics_flattened         = confluent_flink_statement.create_metrics_flattened.statement_name
    velocity_anomaly_alerts   = confluent_flink_statement.create_alerts_table.statement_name
  }
}

output "flink_arima_jobs" {
  description = "Flink ARIMA anomaly detection jobs"
  value = {
    lag_trending_up = confluent_flink_statement.arima_lag_trending_up.statement_name
  }
}

