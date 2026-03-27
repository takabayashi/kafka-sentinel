# Confluent Cloud Kafka Topics - Terraform Configuration

This Terraform configuration provisions 7 Kafka topics on Confluent Cloud for the iFood Platform Team anomaly detection demo.

## Prerequisites

1. **Confluent Cloud Account** with an existing environment and Kafka cluster
2. **Terraform** installed (>= 1.3.0)
3. **Confluent Cloud API Key** (for infrastructure management)

## Topics Provisioned

| Topic | Partitions | Cleanup Policy | Purpose |
|-------|-----------|----------------|---------|
| `simulator_events` | 3 | delete | Synthetic events from data simulator |
| `metrics_source` | 6 | delete | Raw metrics from Velocity Monitor |
| `metrics_flattened` | 6 | delete | Normalized metrics (Flink output) |
| `velocity_anomaly_alerts` | 3 | delete | Anomaly alerts from Flink rules |
| `enriched_alerts` | 3 | delete | AI-enriched alerts with diagnosis |
| `agent_memory` | 6 | **compact** | Historical context for AI Agent |
| `alert_feedback` | 1 | delete | User feedback (thumbs up/down) |

**Note:** `agent_memory` is the only compacted topic, maintaining latest state per consumer group.

## Setup Instructions

### Step 1: Obtain Confluent Cloud Credentials

1. Log into [Confluent Cloud Console](https://confluent.cloud)
2. Navigate to **Account & Access > Cloud API Keys**
3. Click **+ Add API Key** (choose "Global access" for demo)
4. Save the API Key and Secret (you won't see the secret again)

### Step 2: Get Environment and Cluster IDs

1. In Confluent Cloud Console, go to **Environments**
2. Select your environment (or create one)
3. Note the **Environment ID** (format: `env-xxxxx`)
4. Select your Kafka cluster (or create one - **Standard tier** required for Flink ML)
5. Note the **Cluster ID** (format: `lkc-xxxxx`)

### Step 3: Configure Terraform

**Option A: Environment Variables (Recommended)**

```bash
export CONFLUENT_CLOUD_API_KEY="your-cloud-api-key"
export CONFLUENT_CLOUD_API_SECRET="your-cloud-api-secret"
export TF_VAR_environment_id="env-xxxxx"
export TF_VAR_cluster_id="lkc-xxxxx"
```

**Option B: Variables File**

```bash
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values (DO NOT commit this file)
```

### Step 4: Initialize and Apply

```bash
cd infra/terraform

# Initialize Terraform and download providers
terraform init

# Preview changes
terraform plan

# Apply configuration
terraform apply
```

### Step 5: Export Configuration for Applications

```bash
# Export all outputs to JSON
terraform output -json > ../../config/kafka-config.json

# Or export individual values
export KAFKA_BOOTSTRAP=$(terraform output -raw kafka_bootstrap_endpoint)
```

## Verification

After applying, verify topics in Confluent Cloud Console:

1. Navigate to **Environment > Cluster > Topics**
2. Confirm all 7 topics exist
3. Click on `agent_memory` → Configuration → verify `cleanup.policy = compact`

Or use the Confluent CLI:

```bash
confluent kafka topic list --cluster $CLUSTER_ID
```

## Important Notes

### Cluster Tier Requirements

- **Flink ML_DETECT_ANOMALIES** requires **Standard** cluster tier or higher
- Basic tier does NOT support Flink SQL with machine learning

### Kafka API Keys vs Cloud API Keys

- **Cloud API Keys** (used by Terraform): Manage infrastructure (topics, ACLs, connectors)
- **Kafka API Keys** (needed by applications): Access data (produce/consume messages)

You'll need to create **separate Kafka API Keys** for:
- Velocity Monitor
- Data Simulator
- AI Agent
- Dashboard UI

Create these via: **Confluent Cloud Console > Cluster > Data Integration > API Keys**

### Cost Management

Topics incur storage and retention costs. To clean up:

```bash
terraform destroy
```

## Next Steps

After topics are provisioned:

1. **Create Kafka API Keys** for applications
2. **Configure Velocity Monitor** to publish to `metrics_source`
3. **Configure Data Simulator** to publish to `simulator_events`
4. **Set up Flink SQL pipeline** to read from `metrics_source` and write to `velocity_anomaly_alerts`
5. **Deploy AI Agent** to enrich alerts using `agent_memory` context
6. **Launch Dashboard UI** to visualize metrics and alerts

## Troubleshooting

### 401 Unauthorized
- Verify Cloud API Key has correct permissions
- Check API Key is not expired

### Cluster not found
- Verify `cluster_id` format is `lkc-xxxxx`
- Confirm cluster exists in the specified environment

### Topic already exists
Import existing topic to Terraform state:
```bash
terraform import confluent_kafka_topic.topic_name env-xxxxx/lkc-xxxxx/topic-name
```

### Compaction not working
- Verify `agent_memory` config shows `cleanup.policy = compact` in Confluent Cloud UI
- Check log compaction metrics in cluster monitoring

## Support

For issues with:
- **Terraform provider**: https://github.com/confluentinc/terraform-provider-confluent
- **Confluent Cloud**: https://support.confluent.io
- **This project**: See AGENTS.md and CLAUDE.md
