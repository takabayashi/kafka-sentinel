# ================================================================
# Flink ARIMA Jobs - Anomaly Detection
# ================================================================
# Deploy ARIMA-based anomaly detection jobs

# Wait for catalog tables before deploying ARIMA jobs
resource "time_sleep" "wait_for_arima_ready" {
  depends_on      = [time_sleep.wait_for_catalog]
  create_duration = "15s"
}

# ARIMA Job 01: Lag Trending Up
resource "confluent_flink_statement" "arima_lag_trending_up" {
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

  statement      = file("${path.module}/../../flink/jobs-arima/01_arima_lag_trending_up.sql")
  statement_name = "arima-lag-trending-up"
  rest_endpoint  = local.flink_rest_endpoint

  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  properties = {
    "sql.current-catalog"  = var.environment_id
    "sql.current-database" = data.confluent_kafka_cluster.main.id
  }

  depends_on = [time_sleep.wait_for_arima_ready]
}
