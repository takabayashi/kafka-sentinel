# Data Simulator — iFood Kafka Anomaly Detection

Flask API service that generates synthetic Kafka events and injects anomaly patterns for demo purposes.

## Features

### Free Producer Mode
Continuous baseline event generation with configurable throughput:
- Adjustable message rate (msg/sec)
- Establishes baseline for ARIMA model training
- Runs in background until explicitly stopped

### Scenario Modes
Pre-defined anomaly injection patterns:

| Scenario | Effect | Duration |
|----------|--------|----------|
| **lag_spike** | Publishes 45K messages rapidly, creating consumer lag spike | 90s |
| **consumer_slow** | Simulates 10% consumer throughput degradation | 120s |
| **rebalance_storm** | Triggers 15+ rebalances in 2-minute window | 120s |
| **hot_partition** | Routes 80% of traffic to single partition | 90s |

## Setup

### 1. Install Dependencies

```bash
cd simulator
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` and add your Kafka API credentials:
```bash
KAFKA_API_KEY=your-kafka-api-key-here
KAFKA_API_SECRET=your-kafka-api-secret-here
```

**Get Kafka API Keys:**
1. Go to **Confluent Cloud Console > Cluster `lkc-nnrvx6` > API Keys**
2. Click **+ Add API Key**
3. Copy the key and secret to `.env`

### 3. Run the Simulator

```bash
python main.py
```

Server starts on `http://localhost:5001`

## API Endpoints

### Health Check
```bash
GET /health
```

### Get Status
```bash
GET /simulator/status
```

Returns current simulator state (free producer status, active scenario).

### Start Free Producer
```bash
POST /simulator/free-producer/start
Content-Type: application/json

{
  "throughput": 100,
  "consumer_group": "checkout-service",
  "target_topic": "orders"
}
```

### Stop Free Producer
```bash
POST /simulator/free-producer/stop
```

### Run Scenario
```bash
POST /simulator/scenario/lag_spike
Content-Type: application/json

{
  "consumer_group": "checkout-service",
  "target_topic": "orders"
}
```

Available scenarios: `lag_spike`, `consumer_slow`, `rebalance_storm`, `hot_partition`

### Stop All
```bash
POST /simulator/stop
```

## Testing

### Test Free Producer

```bash
# Start producing at 50 msg/s
curl -X POST http://localhost:5001/simulator/free-producer/start \
  -H "Content-Type: application/json" \
  -d '{"throughput": 50}'

# Check status
curl http://localhost:5001/simulator/status

# Stop
curl -X POST http://localhost:5001/simulator/free-producer/stop
```

### Test Lag Spike Scenario

```bash
curl -X POST http://localhost:5001/simulator/scenario/lag_spike \
  -H "Content-Type: application/json" \
  -d '{}'
```

This will:
1. Publish 45K messages rapidly (~10 seconds)
2. Maintain elevated rate for 80 seconds
3. Velocity Monitor detects high lag
4. Flink ARIMA triggers anomaly alert
5. AI Agent enriches with diagnosis
6. Dashboard shows alert within ~30 seconds

## Event Schema

Events published to `simulator_events` topic:

```json
{
  "timestamp": "2025-03-27T12:45:30Z",
  "consumer_group": "checkout-service",
  "topic": "orders",
  "message_count": 1000,
  "event_type": "anomaly_injection",
  "scenario_name": "lag_spike",
  "scenario_id": "lag_spike_a3f9d21c",
  "phase": "burst"
}
```

## Integration with Dashboard

The Dashboard UI will call these endpoints when users click scenario buttons:

```javascript
// Lag Spike button click
fetch('http://localhost:5001/simulator/scenario/lag_spike', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({})
})
```

## Next Steps

1. **Verify events in Confluent Cloud:**
   - Go to Cluster > Topics > `simulator_events`
   - Click Messages tab
   - You should see events appearing

2. **Build Velocity Monitor** to consume these events and publish metrics

3. **Set up Flink Pipeline** for anomaly detection

4. **Wire up Dashboard UI** to trigger scenarios
