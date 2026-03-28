# ================================================================
# Flink Jobs - INSERT Statements
# ================================================================
# Running Flink SQL jobs that transform and analyze data

# Job 01: Formatting - Transform raw metrics and compute derived fields
resource "confluent_flink_statement" "formatting_job" {
  organization {
    id = data.confluent_organization.main.id
  }

  environment {
    id = var.environment_id
  }

  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }

  principal {
    id = confluent_service_account.flink.id
  }

  statement  = file("${path.module}/../../flink/jobs/01_formatting_job.sql")
  statement_name = "formatting-job"
  rest_endpoint = local.flink_rest_endpoint

  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  properties = {
    "sql.current-catalog"  = var.environment_id
    "sql.current-database" = data.confluent_kafka_cluster.main.id
  }

  depends_on = [time_sleep.wait_for_catalog]
}
