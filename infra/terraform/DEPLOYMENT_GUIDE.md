# Flink Deployment Guide

Step-by-step guide to deploy Kafka Sentinel infrastructure including Flink SQL pipeline.

## Quick Reference: Finding Your IDs

### Organization ID
```bash
# Console: Settings → Organization → Organization ID
# Format: org-xxxxx
```

### Environment ID
```bash
# Console: Environments → Click your environment → URL shows env-xxxxx
# Format: env-xxxxx
```

### Cluster ID
```bash
# Console: Environment → Cluster → Cluster overview
# Format: lkc-xxxxx
```

### Service Account ID
```bash
# Console: Accounts & access → Service accounts → Click account
# Format: sa-xxxxx
```

## Step-by-Step Deployment

### 1. Create Flink Service Account

**Why:** Flink statements need a service account to run under.

**Console:**
1. Navigate to **Accounts & access → Service accounts**
2. Click **+ Add service account**
3. Name: `kafka-sentinel-flink`
4. Description: `Service account for Kafka Sentinel Flink statements`
5. Click **Create**
6. **Save the Service Account ID** (sa-xxxxx)

### 2. Grant Flink Permissions

**Why:** Service account needs FlinkDeveloper role to create/run statements.

**Console:**
1. Navigate to your **Environment**
2. Click **Access** tab
3. Click **+ Add role binding**
4. Select service account: `kafka-sentinel-flink`
5. Role: **FlinkDeveloper**
6. Click **Save**

### 3. Configure terraform.tfvars

Create `infra/terraform/terraform.tfvars` with your values:

```hcl
# Cloud API credentials (for Terraform)
confluent_cloud_api_key    = "YOUR_CLOUD_API_KEY"
confluent_cloud_api_secret = "YOUR_CLOUD_API_SECRET"

# Kafka API credentials (for topic creation)
kafka_api_key    = "YOUR_KAFKA_API_KEY"
kafka_api_secret = "YOUR_KAFKA_API_SECRET"

# Resource IDs
environment_id     = "env-xxxxx"     # Your environment ID
cluster_id         = "lkc-xxxxx"     # Your cluster ID
organization_id    = "org-xxxxx"     # Your organization ID
flink_principal_id = "sa-xxxxx"      # Service account ID from step 1

# Optional overrides
# flink_max_cfu = 5  # Default is 5 CFUs
```

### 4. Deploy Everything

```bash
cd infra/terraform

# Initialize (downloads providers)
terraform init

# Preview what will be created
terraform plan

# Expected output:
#   Plan: 15 to add, 0 to change, 0 to destroy.
#   - 7 Kafka topics
#   - 1 Flink compute pool
#   - 7 Flink SQL statements

# Apply
terraform apply

# Type 'yes' when prompted
```

**Timeline:**
- Topics: ~30 seconds
- Flink Compute Pool: ~2-3 minutes
- Flink Statements: ~3-5 minutes total (deployed sequentially)
- **Total:** ~6-9 minutes

### 5. Monitor Deployment

**Watch Flink Compute Pool:**
```bash
watch -n 5 'confluent flink compute-pool list'
```
Wait for status: **PROVISIONED**

**Watch Flink Statements:**
```bash
# Get pool ID from terraform
POOL_ID=$(cd infra/terraform && terraform output -raw flink_compute_pool_id)

# List statements
confluent flink statement list --compute-pool $POOL_ID
```

Expected order:
1. `formatting` (RUNNING)
2. `arima_lag_up` (RUNNING)
3. `arima_speed_down` (RUNNING)
4. `arima_hot_partition` (RUNNING)
5. `threshold_inactive` (RUNNING)
6. `threshold_rebalance` (RUNNING)
7. `correlation_engine` (RUNNING)

### 6. Verify Data Flow

**Start Velocity Monitor:**
```bash
cd velocity-monitor
source venv/bin/activate
python main.py
```

**Check metrics_source has data:**
```bash
confluent kafka topic consume metrics_source --from-beginning --max-messages 5
```

**Check Flink formatted output:**
```bash
confluent kafka topic consume metrics_flattened --from-beginning --max-messages 5
```

### 7. Test Anomaly Detection

**Trigger lag spike:**
```bash
# In simulator terminal
curl -X POST http://localhost:5001/simulator/scenario/lag_spike
```

**Wait 2-3 minutes** (ARIMA needs training data)

**Check for alerts:**
```bash
confluent kafka topic consume velocity_anomaly_alerts --from-beginning
```

You should see alerts like:
```json
{
  "alert_id": "arima_lag_up_my-consumer-group_1234567890",
  "detection_type": "arima_lag_up",
  "severity": "warning",
  "consumer_group": "my-consumer-group",
  "anomaly_score": 0.78,
  ...
}
```

## Troubleshooting

### Flink Compute Pool stuck in PROVISIONING

- **Cause:** Region capacity or quota limits
- **Fix:** Try a different region or request quota increase

### Flink Statement failed with "Table not found"

- **Cause:** Statement deployed before topics were created
- **Fix:** Terraform dependencies should prevent this. If it happens:
  ```bash
  terraform destroy --target=confluent_flink_statement.formatting
  terraform apply
  ```

### "Insufficient permissions" error

- **Cause:** Service account lacks FlinkDeveloper role
- **Fix:** Check environment access (Step 2)

### No data in metrics_flattened

- **Cause:** Velocity monitor not running or formatting statement not running
- **Fix:**
  1. Verify formatting statement status: `RUNNING`
  2. Check velocity-monitor logs
  3. Verify metrics_source has data

### ARIMA not detecting anomalies

- **Cause:** Not enough training data yet (needs ~100 data points)
- **Fix:** Wait 16+ minutes with velocity-monitor running
- **Alternative:** Lower ARIMA `threshold` in SQL files (0.5 → 0.3)

### High Flink costs

- **Cause:** CFUs running 24/7
- **Fix:** Pause statements when not demoing:
  ```bash
  # Pause all statements
  confluent flink statement list --compute-pool $POOL_ID \
    | grep 'RUNNING' \
    | awk '{print $1}' \
    | xargs -I {} confluent flink statement stop --compute-pool $POOL_ID {}

  # Resume later
  confluent flink statement start --compute-pool $POOL_ID <statement-name>
  ```

## Cleanup

**Remove all infrastructure:**
```bash
cd infra/terraform
terraform destroy
```

This will:
1. Stop all Flink statements
2. Delete Flink compute pool
3. Delete all 7 Kafka topics
4. **Data will be lost** (topics are deleted, not compacted)

**Partial cleanup** (keep topics, remove Flink):
```bash
terraform destroy \
  --target=confluent_flink_statement.correlation_engine \
  --target=confluent_flink_statement.threshold_rebalance \
  --target=confluent_flink_statement.threshold_inactive \
  --target=confluent_flink_statement.arima_hot_partition \
  --target=confluent_flink_statement.arima_speed_down \
  --target=confluent_flink_statement.arima_lag_up \
  --target=confluent_flink_statement.formatting \
  --target=confluent_flink_compute_pool.main
```

## Cost Estimation

**Flink Compute Pool:**
- 5 CFUs × $1.50/hour = $7.50/hour
- Running 24/7: ~$5,400/month
- **Recommendation:** Pause when not demoing

**Standard Cluster + Topics:**
- Varies by region and usage
- Typical demo: $50-200/month

**Total (running continuously):** ~$5,450-5,600/month
**Total (paused overnight):** ~$1,000-1,500/month

## Next Steps

After successful deployment:

1. **AI Agent** - Build Confluent Intelligence agent for alert enrichment
2. **Dashboard** - Deploy React UI to visualize alerts
3. **Feedback Loop** - Collect engineer feedback via alert_feedback topic
4. **Production Hardening** - Add monitoring, alerting, and SLOs
