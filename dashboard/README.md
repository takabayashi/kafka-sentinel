# Kafka Anomaly Detection Dashboard

Real-time dashboard for monitoring Kafka consumer group metrics and AI-enriched anomaly alerts.

## Architecture

- **Backend**: Node.js WebSocket server that consumes from Kafka and streams to frontend
- **Frontend**: React app with three panels (Metrics View, Alert Feed, Simulator Panel)
- **Communication**: WebSocket for real-time bidirectional updates

## Setup

### Backend

```bash
cd backend
npm install
cp .env.example .env
# Edit .env with your Confluent Cloud credentials
npm run dev
```

### Frontend

```bash
cd frontend
npm install
npm run dev
```

The dashboard will be available at http://localhost:3000

## Features

### Metrics View
- Real-time consumer group metrics (lag, read/write speed, time to catch up)
- Historical trend charts
- Auto-updates as new metrics arrive

### Alert Feed
- Displays anomaly alerts with AI-generated diagnoses
- Color-coded severity (high/medium/low)
- Expandable details with recommended actions
- Thumbs up/down feedback mechanism

### Simulator Panel
- Four scenario buttons: Lag Spike, Consumer Slow, Rebalance Storm, Hot Partition
- Click to inject synthetic anomalies
- 5-second cooldown between scenarios

## Topics Consumed

- `metrics_flattened` - Consumer group metrics
- `velocity_anomaly_alerts` - Raw alerts from Flink
- `enriched_alerts` - AI-enriched alerts with diagnosis

## Topics Produced

- `alert_feedback` - User feedback (thumbs up/down)
- `simulator_commands` - Simulator scenario triggers

## Environment Variables

```bash
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
KAFKA_API_KEY=your-api-key
KAFKA_API_SECRET=your-api-secret
WEBSOCKET_PORT=8080
```

Frontend uses `VITE_WS_URL=ws://localhost:8080` (configurable in .env)
