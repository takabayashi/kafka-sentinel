# ================================================================
# Flink Catalog - Table Definitions
# ================================================================
# Create tables in Flink catalog (non-running, just DDL)

resource "time_sleep" "wait_for_flink_ready" {
  depends_on = [
    confluent_flink_compute_pool.main,
    confluent_role_binding.flink_developer,
    confluent_role_binding.flink_kafka_read,
    confluent_role_binding.flink_kafka_write,
    confluent_role_binding.flink_schema_registry_read,
    confluent_role_binding.flink_schema_registry_write
  ]
  create_duration = "60s"
}

# Create metrics_source table
resource "confluent_flink_statement" "create_metrics_source" {
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

  statement     = file("${path.module}/../../flink/catalog/01_create_metrics_source.sql")
  rest_endpoint = local.flink_rest_endpoint

  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  properties = {
    "sql.current-catalog"  = var.environment_id
    "sql.current-database" = data.confluent_kafka_cluster.main.id
  }

  depends_on = [
    time_sleep.wait_for_flink_ready,
    confluent_kafka_topic.metrics_source
  ]
}

# Create metrics_flattened table
resource "confluent_flink_statement" "create_metrics_flattened" {
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

  statement     = file("${path.module}/../../flink/catalog/02_create_metrics_flattened.sql")
  rest_endpoint = local.flink_rest_endpoint

  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  properties = {
    "sql.current-catalog"  = var.environment_id
    "sql.current-database" = data.confluent_kafka_cluster.main.id
  }

  depends_on = [
    confluent_flink_statement.create_metrics_source,
    confluent_kafka_topic.metrics_flattened
  ]
}

# Create velocity_anomaly_alerts table
resource "confluent_flink_statement" "create_alerts_table" {
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

  statement     = file("${path.module}/../../flink/catalog/03_create_velocity_anomaly_alerts.sql")
  rest_endpoint = local.flink_rest_endpoint

  credentials {
    key    = var.flink_api_key
    secret = var.flink_api_secret
  }

  properties = {
    "sql.current-catalog"  = var.environment_id
    "sql.current-database" = data.confluent_kafka_cluster.main.id
  }

  depends_on = [
    confluent_flink_statement.create_metrics_flattened,
    confluent_kafka_topic.velocity_anomaly_alerts
  ]
}

resource "time_sleep" "wait_for_catalog" {
  depends_on      = [confluent_flink_statement.create_alerts_table]
  create_duration = "15s"
}
