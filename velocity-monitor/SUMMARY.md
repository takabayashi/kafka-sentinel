# Kafka Sentinel - Project Summary

## What is Kafka Sentinel?

**Kafka Sentinel** is an AI-powered real-time anomaly detection system for Kafka infrastructure. It continuously monitors your Kafka cluster, detects performance issues before they become critical, and provides AI-generated natural language explanations with recommended actions.

## Key Capabilities

✅ **Sub-minute detection** - Polls Consumer Group API every 10s for real-time lag detection  
✅ **ARIMA time-series analysis** - Detects trending anomalies (lag increasing, speed decreasing)  
✅ **Threshold-based rules** - Catches immediate issues (consumer inactive, rebalance storm)  
✅ **Root cause correlation** - Distinguishes producer vs consumer problems  
✅ **AI diagnosis** - Natural language explanations via Confluent Intelligence  
✅ **Demo scenarios** - Pre-built patterns for testing and demonstration  

## Architecture Components

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Infrastructure | Terraform | Provision Kafka topics on Confluent Cloud |
| Data Simulator | Python/Flask | Generate events and inject anomaly scenarios |
| Velocity Monitor | Python | Poll APIs, compute metrics, publish to Kafka |
| Flink Pipeline | Flink SQL | ARIMA + threshold anomaly detection |
| AI Agent | Confluent Intelligence | Enrich alerts with diagnosis |
| Dashboard | React | Visualize metrics and alerts |

## Current Status

✅ Infrastructure, Simulator, Velocity Monitor  
🔄 Flink SQL Pipeline  
📋 AI Agent, Dashboard

## Target Use Cases

- **Platform teams** monitoring Kafka infrastructure health
- **SRE teams** reducing MTTR for Kafka incidents
- **Demo environments** showing AI-powered observability
- **Learning** how to build real-time anomaly detection systems

## Tech Stack

- **Confluent Cloud** (Kafka as a Service, Standard tier+)
- **Apache Flink** (Stream processing with ML_DETECT_ANOMALIES)
- **Python** (Simulator, Velocity Monitor)
- **Confluent Intelligence** (AI-powered diagnosis)
- **Terraform** (Infrastructure as Code)
- **React** (Dashboard UI)

## License

Apache 2.0 - Open source and free to use
