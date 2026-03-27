provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

# Fetch existing Kafka cluster metadata
data "confluent_kafka_cluster" "main" {
  id = var.cluster_id
  environment {
    id = var.environment_id
  }
}
