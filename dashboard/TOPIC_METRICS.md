# Topic Metrics - Confluent Cloud Metrics API

## Overview

O painel **Topic Metrics** fornece métricas em tempo real de todos os tópicos Kafka usando a Confluent Cloud Metrics API.

## Métricas Disponíveis

### Por Tópico
- **Received Bytes/s** - Taxa de escrita dos produtores
- **Sent Bytes/s** - Taxa de leitura dos consumidores
- **Received Records/s** - Mensagens recebidas por segundo
- **Sent Records/s** - Mensagens enviadas por segundo
- **Retained Bytes** - Tamanho atual do tópico

### Por Cluster
- **Cluster Throughput** - Throughput total (recebido + enviado)
- **Active Connections** - Conexões ativas no cluster
- **Cluster ID** - Identificador do cluster

## Como Habilitar

### 1. Criar Cloud API Keys

As métricas de tópicos requerem **Cloud API Keys** (diferentes das Kafka cluster API keys):

```bash
# Via Confluent Cloud UI
1. Acesse https://confluent.cloud/settings/api-keys
2. Clique em "Add key"
3. Selecione "Cloud resource management"
4. Dê um nome: "metrics-api-dashboard"
5. Copie a API Key e Secret
```

Ou via CLI:
```bash
confluent api-key create --resource cloud
```

### 2. Configurar Credenciais

Adicione as Cloud API Keys no arquivo `.env`:

```bash
# dashboard/backend/.env
CONFLUENT_CLOUD_API_KEY=your-cloud-api-key-here
CONFLUENT_CLOUD_API_SECRET=your-cloud-api-secret-here
```

### 3. Reiniciar o Backend

```bash
# Parar serviços
pkill -f "server.js"

# Reiniciar
cd dashboard/backend
npm start
```

Ou reiniciar tudo:
```bash
make dev
```

Você verá no log:
```
✅ Metrics API enabled - will fetch topic metrics every 60s
```

## Diferença: Cloud API Keys vs Kafka API Keys

| Tipo | Uso | Onde Criar |
|------|-----|------------|
| **Kafka Cluster API Keys** | Produzir/Consumir mensagens, REST API v3 | Confluent Cloud > Cluster > API Keys |
| **Cloud API Keys** | Metrics API, Cloud resource management | Confluent Cloud > Settings > API Keys |

## Atualização de Dados

- **Topic Metrics**: Atualizadas a cada **60 segundos**
- **Cluster Metrics**: Atualizadas a cada **120 segundos**
- **Granularidade**: 1 minuto (últimos 15 minutos)

## Visualização no Dashboard

1. Acesse http://localhost:3000 (ou 3001)
2. Clique na aba **"Topic Metrics"**
3. Veja:
   - **Resumo do Cluster** no topo
   - **Cards por Tópico** com métricas individuais
   - **Clique em um card** para ver mini-gráfico de tendência (últimos 15 min)
   - **Gráfico Comparativo** de throughput entre tópicos

## Troubleshooting

### Erro 401 - Unauthorized
```
Error: Metrics API error: 401 - Credentials are required
```

**Solução**: Verifique se as Cloud API Keys estão corretas no `.env`

### Métricas não aparecem

1. Verifique o log do backend:
```bash
tail -f /tmp/dashboard-startup.log | grep Metrics
```

2. Deve mostrar:
```
✅ Metrics API enabled - will fetch topic metrics every 60s
```

3. Se mostrar:
```
⚠️  Metrics API disabled - set CONFLUENT_CLOUD_API_KEY...
```

Então as credenciais não foram configuradas.

### Permissões Insuficientes

As Cloud API Keys precisam ter permissão para acessar métricas. Verifique:
```bash
confluent api-key describe <key-id>
```

## API Reference

Documentação oficial da Confluent Cloud Metrics API:
- https://docs.confluent.io/cloud/current/monitoring/metrics-api.html

## Exemplo de Resposta

```json
{
  "topic": "metrics_source",
  "received_bytes": 1024.5,
  "sent_bytes": 2048.3,
  "received_records": 150,
  "sent_records": 150,
  "retained_bytes": 1048576,
  "timeseries": {
    "received_bytes": [
      { "timestamp": "2026-03-27T14:00:00Z", "value": 1024 },
      { "timestamp": "2026-03-27T14:01:00Z", "value": 1050 },
      ...
    ]
  },
  "timestamp": "2026-03-27T14:15:00Z"
}
```

## Limitações

- Delay de ~3-5 minutos (natureza da Metrics API)
- Granularidade mínima: 1 minuto
- Retenção: 7 dias
- Rate limit: 60 requests/minute

Para métricas **real-time** (sub-segundo), use o painel **Consumer Metrics** que lê diretamente da Consumer Group API.
