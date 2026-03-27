# Kafka Real-Time Anomaly Detection with AI

Real-time Kafka infrastructure anomaly detection using Confluent Cloud, Flink SQL, and AI-powered alert enrichment.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## Overview

Demonstrates how to build a real-time anomaly detection system for Kafka infrastructure that detects performance degradations, explains problems in natural language, and reduces MTTR.

### Key Features

- 🔍 Real-time detection (sub-minute alerts)
- 📊 ARIMA time-series + threshold-based rules  
- 🤖 AI-powered natural language diagnosis
- 🎯 Root cause correlation (producer vs consumer)
- 📈 Pre-built demo scenarios

## Architecture

```
Simulator → Velocity Monitor → Flink → AI Agent → Dashboard
```

Components: Terraform, Python/Flask, Flink SQL, Confluent Intelligence, React

## Quick Start

See component README files:
- `infra/terraform/README.md` - Infrastructure setup
- `simulator/README.md` - Data generator
- `velocity-monitor/README.md` - Metrics collector

## Important: API Credentials

⚠️ **Kafka API Keys** (NOT Cloud API keys) for applications:
- Get from: Confluent Cloud Console > Cluster > API Keys
- Used for: REST API v3 calls AND Kafka produce/consume
- Set as: `KAFKA_API_KEY` and `KAFKA_API_SECRET` in `.env`

**Cloud API Keys** only for Terraform.

## Development Status

✅ Infrastructure, Simulator, Velocity Monitor  
🔄 Flink SQL Pipeline  
📋 AI Agent, Dashboard UI

## License

Apache License 2.0 - see [LICENSE](LICENSE)
