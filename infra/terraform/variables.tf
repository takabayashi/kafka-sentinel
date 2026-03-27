variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API Key for Terraform (not Kafka API Key)"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API Secret for Terraform"
  type        = string
  sensitive   = true
}

variable "kafka_api_key" {
  description = "Kafka API Key for managing topics via REST API"
  type        = string
  sensitive   = true
}

variable "kafka_api_secret" {
  description = "Kafka API Secret for managing topics"
  type        = string
  sensitive   = true
}

variable "environment_id" {
  description = "Confluent Cloud Environment ID (e.g., env-xxxxx)"
  type        = string
}

variable "cluster_id" {
  description = "Confluent Cloud Kafka Cluster ID (e.g., lkc-xxxxx)"
  type        = string
}

variable "topic_retention_ms" {
  description = "Default retention period for regular topics in milliseconds (7 days)"
  type        = string
  default     = "604800000"
}

variable "environment" {
  description = "Environment name for resource tagging"
  type        = string
  default     = "demo"
}

variable "project_name" {
  description = "Project name for resource tagging"
  type        = string
  default     = "ifood-anomaly-detection"
}
