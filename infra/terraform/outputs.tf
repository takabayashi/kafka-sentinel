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
