# ================================================================
# Flink Compute Pool
# ================================================================
# Note: Flink SQL statements are deployed via CLI script due to
# Terraform provider authentication issues. See:
# infra/deploy-flink-statements.sh

# Construct Flink REST endpoint from cluster metadata
locals {
  flink_rest_endpoint = "https://flink.${data.confluent_kafka_cluster.main.region}.${lower(data.confluent_kafka_cluster.main.cloud)}.confluent.cloud"
}

# Flink Compute Pool
resource "confluent_flink_compute_pool" "main" {
  display_name = "kafka-sentinel-compute"
  cloud        = data.confluent_kafka_cluster.main.cloud
  region       = data.confluent_kafka_cluster.main.region
  max_cfu      = var.flink_max_cfu

  environment {
    id = var.environment_id
  }
}

# Wait for compute pool to be ready before running deploy-flink-statements.sh
resource "time_sleep" "wait_for_compute_pool" {
  depends_on      = [confluent_flink_compute_pool.main]
  create_duration = "30s"
}
