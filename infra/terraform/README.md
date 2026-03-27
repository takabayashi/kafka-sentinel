# Confluent Cloud Infrastructure - Terraform Configuration

This Terraform configuration provisions the complete infrastructure for Kafka Sentinel:
- 7 Kafka topics
- Flink Compute Pool
- 7 Flink SQL anomaly detection statements

## Prerequisites

1. **Confluent Cloud Account** with an existing environment
2. **Kafka Cluster** - **Standard tier or higher** (required for Flink ML_DETECT_ANOMALIES)
3. **Terraform** installed (>= 1.3.0)
4. **Confluent Cloud API Key** (for infrastructure management)
5. **Kafka API Key** (cluster-level, for topic creation)

**Note:** Terraform will automatically create:
- Flink Service Account
- Role bindings (FlinkDeveloper, DeveloperRead, DeveloperWrite, Schema Registry access)

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

### Step 2: Get Resource IDs

1. **Environment ID**: Confluent Cloud Console → Environments → Note `env-xxxxx`
2. **Cluster ID**: Select your environment → Cluster → Note `lkc-xxxxx`

### Step 3: Create Kafka API Keys

1. Navigate to your cluster → **Data Integration → API Keys**
2. Click **+ Add API Key**
3. Select scope: **Global access** (for demo) or specific topics
4. Save the key and secret

### Step 4: Configure Terraform

**Option A: Environment Variables (Recommended)**

```bash
export CONFLUENT_CLOUD_API_KEY="your-cloud-api-key"
export CONFLUENT_CLOUD_API_SECRET="your-cloud-api-secret"
export TF_VAR_kafka_api_key="your-kafka-api-key"
export TF_VAR_kafka_api_secret="your-kafka-api-secret"
export TF_VAR_environment_id="env-xxxxx"
export TF_VAR_cluster_id="lkc-xxxxx"
```

**Option B: Variables File**

```bash
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with all required values (DO NOT commit this file)
```

### Step 5: Deploy Infrastructure

```bash
cd infra/terraform

# Initialize Terraform and download providers
terraform init

# Preview changes (creates: 7 topics + 1 compute pool + 7 Flink statements)
terraform plan

# Apply configuration
terraform apply

# NOTE: Flink compute pool takes ~2-3 minutes to provision
# Flink statements will deploy sequentially after pool is ready
```

**Expected resources created:**
- 1 Service Account (kafka-sentinel-flink)
- 4 Role Bindings (FlinkDeveloper, DeveloperRead, DeveloperWrite, Schema Registry)
- 7 Kafka topics
- 1 Flink Compute Pool (5 CFUs max)
- 7 Flink SQL statements (1 formatting + 6 detection rules)

**Total: 20 resources**

### Step 6: Export Configuration for Applications

```bash
# Export all outputs to JSON
terraform output -json > ../../config/kafka-config.json

# Or export individual values
export KAFKA_BOOTSTRAP=$(terraform output -raw kafka_bootstrap_endpoint)
export FLINK_POOL_ID=$(terraform output -raw flink_compute_pool_id)
```

## Verification

### Verify Kafka Topics

1. **Confluent Cloud Console**: Environment → Cluster → Topics
   - Confirm all 7 topics exist
   - Click `agent_memory` → verify `cleanup.policy = compact`

2. **CLI**:
   ```bash
   confluent kafka topic list --cluster $CLUSTER_ID
   ```

### Verify Flink Compute Pool

1. **Console**: Environment → Flink → Compute Pools
   - Status should be **Running**
   - Name: `kafka-sentinel-compute`

2. **CLI**:
   ```bash
   confluent flink compute-pool list
   ```

### Verify Flink Statements

1. **Console**: Environment → Flink → Statements
   - Should show 7 statements, all **RUNNING**
   - Order: formatting → 6 detection rules

2. **CLI**:
   ```bash
   confluent flink statement list --compute-pool $FLINK_POOL_ID
   ```

### Test Data Flow

1. Start **velocity-monitor** to publish metrics
2. Wait ~30 seconds for data to flow
3. Check `metrics_flattened` topic has data:
   ```bash
   confluent kafka topic consume metrics_flattened --from-beginning | head -10
   ```
4. Trigger anomaly with **simulator** lag_spike scenario
5. Wait ~2 minutes (ARIMA training window)
6. Check for alerts:
   ```bash
   confluent kafka topic consume velocity_anomaly_alerts --from-beginning
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
