# Kafka Anomaly Detection System

Real-time Kafka infrastructure anomaly detection system using Confluent Cloud, Flink SQL, and AI-powered diagnostics.

## Quick Start

```bash
# 1. Install all dependencies
make setup

# 2. Configure environment files
make config-all
# Edit .env files with your Confluent Cloud credentials

# 3. Start all components
make dev
```

Dashboard will be available at http://localhost:3000

## Components

### Data Simulator
Generates synthetic Kafka events and injects anomaly scenarios for demo purposes.

```bash
make run-simulator
```

### Velocity Monitor
Polls Confluent Cloud APIs to collect real-time consumer group metrics.

```bash
make run-velocity-monitor
```

### Dashboard
React UI with real-time metrics, AI-enriched alerts, and anomaly simulator controls.

```bash
make run-dashboard          # Start both backend and frontend
make run-dashboard-backend   # WebSocket server only
make run-dashboard-frontend  # React UI only
```

### Flink Pipeline
Runs on Confluent Cloud - see `flink/` directory for SQL statements.

### AI Agent
Uses Confluent Intelligence - see `AGENTS.md` for configuration.

## Architecture

```
Data Simulator → Velocity Monitor → [metrics_source] → Flink Pipeline → [velocity_anomaly_alerts]
                                                                                ↓
                                                                          AI Agent
                                                                                ↓
                                                                      [enriched_alerts]
                                                                                ↓
                                                                          Dashboard UI
```

## Makefile Commands

Run `make help` to see all available commands:

```bash
make setup                    # Install all dependencies
make config-all               # Copy all .env.example files
make dev                      # Start all components
make clean                    # Remove virtual envs and node_modules
make check-config             # Verify .env files exist
make check-deps               # Verify dependencies installed
```

## Topics

| Topic | Description |
|-------|-------------|
| `simulator_events` | Synthetic events from data simulator |
| `metrics_source` | Raw consumer group metrics |
| `metrics_flattened` | Normalized metrics (post-Flink) |
| `velocity_anomaly_alerts` | Alerts from Flink detection rules |
| `enriched_alerts` | AI-enriched alerts with diagnosis |
| `agent_memory` | Historical context (compacted) |
| `alert_feedback` | User feedback from dashboard |

## Demo Flow

1. Open dashboard at http://localhost:3000
2. Click "Lag Spike" scenario button
3. Watch metrics update in real-time
4. Alert appears ~30 seconds later with AI diagnosis
5. Provide feedback with 👍 or 👎

## Development

See component-specific READMEs:
- [Simulator](simulator/README.md)
- [Velocity Monitor](velocity-monitor/README.md)
- [Dashboard](dashboard/README.md)
- [Flink Pipeline](flink/README.md)

For detailed architecture and implementation guidance, see [CLAUDE.md](CLAUDE.md).
