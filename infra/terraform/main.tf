provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

# Fetch existing environment
data "confluent_environment" "main" {
  id = var.environment_id
}

# Fetch existing Kafka cluster metadata
data "confluent_kafka_cluster" "main" {
  id = var.cluster_id
  environment {
    id = var.environment_id
  }
}

# Fetch organization details
data "confluent_organization" "main" {}

# Fetch Schema Registry cluster
data "confluent_schema_registry_cluster" "main" {
  environment {
    id = var.environment_id
  }
}

