# Dashboard Guide - Kafka Anomaly Detection

## 🚀 Quick Start

```bash
# Start all services
make dev

# Dashboard will be available at:
# http://localhost:3000 (or http://localhost:3001 if 3000 is busy)
```

## 📊 Dashboard Features

### Três Painéis Principais

#### 1. **Metrics View** (Painel Esquerdo)
Duas abas com métricas complementares:

**Aba "Consumer Metrics"** ✅ *Funcionando*
- Métricas em tempo real de consumer groups
- Atualização: **Tempo real** via Kafka Consumer Group API
- Delay: **< 10 segundos**
- Dados exibidos:
  - Current Lag
  - Read Speed (msg/s)
  - Write Speed (msg/s)
  - Time to Catch Up
- Gráficos históricos (últimos 50 pontos)

**Aba "Topic Metrics"** ⚠️ *Requer Cloud API Keys*
- Métricas de throughput de todos os tópicos
- Atualização: **60 segundos** via Confluent Cloud Metrics API
- Delay: **3-5 minutos** (limitação da API)
- Dados exibidos:
  - Received/Sent Bytes per second
  - Received/Sent Records per second
  - Retained Bytes (tamanho do tópico)
  - Cluster throughput total
  - Active connections
- Gráficos de tendência (últimos 15 minutos)
- Mini-gráficos ao clicar nos cards

#### 2. **Alert Feed** (Painel Superior Direito)
- Exibe alertas de anomalias detectadas
- Color-coded por severidade (High/Medium/Low)
- Mostra diagnóstico da IA (quando disponível)
- Expandível para detalhes completos
- Feedback com 👍/👎 → publica em `alert_feedback` topic

#### 3. **Simulator Panel** (Painel Inferior Direito)
Quatro cenários de anomalia pré-configurados:

- **📈 Lag Spike** - Aumento súbito no lag do consumidor
- **🐌 Consumer Slow** - Degradação gradual do throughput
- **🌪️ Rebalance Storm** - Múltiplos rebalanceamentos consecutivos
- **🔥 Hot Partition** - Distribuição desigual de partições

Cada botão injeta o cenário correspondente via `simulator_commands` topic.

## 🔌 Arquitetura WebSocket

```
Dashboard Frontend (React)
        ↕ WebSocket
Dashboard Backend (Node.js)
        ↕ Kafka Consumer
Topics: metrics_flattened, velocity_anomaly_alerts, enriched_alerts
```

### Fluxo de Dados

1. **Velocity Monitor** → publica em `metrics_source` (a cada 10s)
2. **Flink** → processa e publica em `metrics_flattened` + `velocity_anomaly_alerts`
3. **AI Agent** → enriquece e publica em `enriched_alerts`
4. **Dashboard Backend** → consome tópicos e transmite via WebSocket
5. **Dashboard Frontend** → recebe updates em tempo real

## 📡 WebSocket Messages

### Mensagens Recebidas (Backend → Frontend)

```javascript
// Métrica de consumer group
{
  "topic": "metrics_flattened",
  "data": {
    "key": "checkout-service",
    "value": {
      "consumer_group": "checkout-service",
      "current_lag": 1234,
      "read_speed_msg_per_sec": 100,
      "write_speed_msg_per_sec": 105,
      ...
    }
  }
}

// Alerta de anomalia (raw)
{
  "topic": "velocity_anomaly_alerts",
  "data": {
    "value": {
      "alert_id": "alert-123",
      "consumer_group": "checkout-service",
      "anomaly_type": "Lag Trending Up",
      ...
    }
  }
}

// Alerta enriquecido com IA
{
  "topic": "enriched_alerts",
  "data": {
    "value": {
      "alert_id": "alert-123",
      "diagnosis": "Consumer group lag spiked...",
      "severity": "HIGH",
      "recommended_action": "Scale consumer group...",
      ...
    }
  }
}

// Métricas de tópicos (se Cloud API Keys configuradas)
{
  "topic": "topic_metrics",
  "data": {
    "value": [
      {
        "topic": "metrics_source",
        "received_bytes": 1024,
        "sent_bytes": 2048,
        ...
      }
    ]
  }
}
```

### Mensagens Enviadas (Frontend → Backend)

```javascript
// Trigger cenário do simulador
{
  "type": "simulator_action",
  "payload": {
    "scenario": "lag_spike"
  }
}

// Feedback de alerta
{
  "type": "alert_feedback",
  "payload": {
    "alert_id": "alert-123",
    "feedback": "up" // ou "down"
  }
}

// Requisição de métricas (sob demanda)
{
  "type": "request_metrics",
  "payload": {
    "type": "all_topics" // ou "topic" ou "cluster"
  }
}
```

## 🛠️ Configuração

### Backend (.env)

```bash
# Kafka cluster
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-2.aws.confluent.cloud:9092

# Kafka Cluster API Keys (obrigatório)
KAFKA_API_KEY=your-kafka-api-key
KAFKA_API_SECRET=your-kafka-api-secret

# Cloud API Keys (opcional - para Topic Metrics)
CONFLUENT_CLOUD_API_KEY=your-cloud-api-key
CONFLUENT_CLOUD_API_SECRET=your-cloud-api-secret

# WebSocket
WEBSOCKET_PORT=8080
```

### Frontend (.env)

```bash
VITE_WS_URL=ws://localhost:8080
```

## 🎨 Componentes React

```
App.jsx
├── MetricsView.jsx        # Consumer group metrics + charts
├── TopicMetrics.jsx       # Topic-level metrics (Confluent Metrics API)
├── AlertFeed.jsx          # Alert list with AI diagnosis
└── SimulatorPanel.jsx     # Anomaly scenario buttons
```

## 🔄 Ciclo de Vida

### Startup
1. Backend conecta ao Kafka como consumidor
2. Backend inicia WebSocket server na porta 8080
3. Backend se inscreve nos tópicos: `metrics_flattened`, `velocity_anomaly_alerts`, `enriched_alerts`
4. Frontend conecta ao WebSocket
5. Backend inicia timers para Metrics API (se configurado)

### Runtime
1. Mensagens chegam do Kafka → Backend faz broadcast via WebSocket
2. Frontend atualiza UI em tempo real
3. Usuário clica em cenário → Frontend envia comando → Backend publica no Kafka
4. Usuário dá feedback → Frontend envia → Backend publica em `alert_feedback`

### Shutdown
1. Ctrl+C no terminal onde `make dev` está rodando
2. Backend faz flush do Kafka producer
3. Backend fecha conexões WebSocket
4. Todos os processos terminam gracefully

## 📊 Performance

- **Consumer Metrics**: Atualização < 10s (real-time via Consumer Group API)
- **Topic Metrics**: Atualização 60s (delay 3-5min da Metrics API)
- **WebSocket**: Latência < 50ms
- **Memory**: ~200MB (backend + frontend)

## 🐛 Troubleshooting

### Dashboard não carrega
```bash
# Verificar portas
lsof -i:3000,3001,8080

# Verificar logs
tail -f /tmp/dashboard-startup.log
```

### WebSocket não conecta
```bash
# Verificar backend
curl http://localhost:8080
# Deve retornar erro (HTTP, não WebSocket)

# Testar WebSocket
wscat -c ws://localhost:8080
```

### Métricas não aparecem
```bash
# Verificar Kafka consumer
# Logs devem mostrar: [metrics_flattened] consumer_group_name

# Verificar tópicos
kafka-console-consumer --bootstrap-server ... --topic metrics_flattened
```

### Topic Metrics retornam 401
```
Error: Metrics API error: 401 - Credentials are required
```
**Solução**: Configurar `CONFLUENT_CLOUD_API_KEY` e `CONFLUENT_CLOUD_API_SECRET` no `dashboard/backend/.env`

Ver: `dashboard/TOPIC_METRICS.md`

## 🚦 Status dos Componentes

| Componente | Status | Porta | Tópico Consumido |
|------------|--------|-------|------------------|
| Dashboard Frontend | ✅ Running | 3000/3001 | - |
| Dashboard Backend | ✅ Running | 8080 (WS) | metrics_flattened, velocity_anomaly_alerts, enriched_alerts |
| Velocity Monitor | ✅ Running | - | - (produz em metrics_source) |
| Simulator API | ✅ Running | 5001 | simulator_commands |
| Topic Metrics | ⚠️ Opcional | - | - (via Metrics API) |

## 📚 Próximos Passos

1. ✅ Dashboard UI completo
2. 🔄 Deploy Flink SQL Pipeline (detectar anomalias)
3. 🔄 Configurar AI Agent (Confluent Intelligence)
4. ✅ Testar cenários do simulador
5. 📊 Adicionar mais gráficos (opcional)

## 🔗 Links Úteis

- Confluent Cloud Metrics API: https://docs.confluent.io/cloud/current/monitoring/metrics-api.html
- Confluent Intelligence: https://docs.confluent.io/cloud/current/flink/
- Recharts Documentation: https://recharts.org/
