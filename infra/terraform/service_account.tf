# ================================================================
# Flink Service Account and Role Bindings
# ================================================================

# Create service account for Flink statements
resource "confluent_service_account" "flink" {
  display_name = "kafka-sentinel-flink"
  description  = "Service account for Kafka Sentinel Flink SQL statements"
}

# Grant FlinkDeveloper role to service account in environment
resource "confluent_role_binding" "flink_developer" {
  principal   = "User:${confluent_service_account.flink.id}"
  role_name   = "FlinkDeveloper"
  crn_pattern = data.confluent_environment.main.resource_name
}

# Grant access to read from Kafka topics (for Flink sources)
resource "confluent_role_binding" "flink_kafka_read" {
  principal   = "User:${confluent_service_account.flink.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.main.rbac_crn}/kafka=${data.confluent_kafka_cluster.main.id}/topic=*"
}

# Grant access to write to Kafka topics (for Flink sinks)
resource "confluent_role_binding" "flink_kafka_write" {
  principal   = "User:${confluent_service_account.flink.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_kafka_cluster.main.rbac_crn}/kafka=${data.confluent_kafka_cluster.main.id}/topic=*"
}

# Grant access to Schema Registry (for json-registry format in Flink tables)
resource "confluent_role_binding" "flink_schema_registry_read" {
  principal   = "User:${confluent_service_account.flink.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.main.resource_name}/subject=*"
}

resource "confluent_role_binding" "flink_schema_registry_write" {
  principal   = "User:${confluent_service_account.flink.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${data.confluent_schema_registry_cluster.main.resource_name}/subject=*"
}

# Note: Flink API key and Schema Registry API key are provided via variables
# and used directly in Flink statement credentials blocks
# No service account API keys created here - using user-provided keys instead
