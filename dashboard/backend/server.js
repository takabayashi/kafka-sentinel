import { Kafka } from 'kafkajs';
import { WebSocketServer } from 'ws';
import dotenv from 'dotenv';
import { readFileSync } from 'fs';
import { MetricsAPIClient } from './metrics-api.js';

dotenv.config();

// Load Kafka config
const kafkaConfig = JSON.parse(
  readFileSync('../../config/kafka-config.json', 'utf-8')
);

const kafka = new Kafka({
  clientId: 'dashboard-backend',
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS],
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_API_KEY,
    password: process.env.KAFKA_API_SECRET,
  },
});

const consumer = kafka.consumer({ groupId: 'dashboard-ui-group' });
const producer = kafka.producer();

// Initialize Metrics API client
// Note: Metrics API requires Cloud API Keys, NOT Kafka cluster API keys
const metricsClient = new MetricsAPIClient(
  process.env.CONFLUENT_CLOUD_API_KEY || process.env.KAFKA_API_KEY,
  process.env.CONFLUENT_CLOUD_API_SECRET || process.env.KAFKA_API_SECRET,
  kafkaConfig.cluster_id.value
);

const wss = new WebSocketServer({ port: process.env.WEBSOCKET_PORT || 8080 });

const clients = new Set();

// Topic list for metrics
const TOPICS = Object.values(kafkaConfig.topic_names.value);

wss.on('connection', (ws) => {
  const clientId = Math.random().toString(36).substr(2, 9);
  console.log(`[${clientId}] Client connected`);
  clients.add(ws);

  ws.on('close', (code, reason) => {
    console.log(`[${clientId}] Client disconnected (code: ${code})`);
    clients.delete(ws);
  });

  ws.on('error', (error) => {
    console.error(`[${clientId}] WebSocket error:`, error.message);
  });

  ws.on('message', async (message) => {
    try {
      const data = JSON.parse(message);
      console.log(`[${clientId}] Received:`, data.type);

      // Handle simulator commands
      if (data.type === 'simulator_action') {
        await handleSimulatorAction(data.payload);
      }

      // Handle simulator stop
      if (data.type === 'simulator_stop') {
        await handleSimulatorStop();
      }

      // Handle producer start
      if (data.type === 'producer_start') {
        await handleProducerStart(data.payload);
      }

      // Handle producer stop
      if (data.type === 'producer_stop') {
        await handleProducerStop();
      }

      // Handle feedback
      if (data.type === 'alert_feedback') {
        await handleFeedback(data.payload);
      }

      // Handle metrics request
      if (data.type === 'request_metrics') {
        console.log(`[${clientId}] Fetching metrics for:`, data.payload.type);
        await handleMetricsRequest(data.payload, ws);
      }
    } catch (error) {
      console.error(`[${clientId}] Error handling message:`, error);
    }
  });
});

async function handleSimulatorAction(payload) {
  console.log('[Simulator] Triggering scenario:', payload.scenario);

  try {
    const SIMULATOR_URL = process.env.SIMULATOR_URL || 'http://localhost:5001';
    const response = await fetch(`${SIMULATOR_URL}/simulator/scenario/${payload.scenario}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        consumer_group: 'checkout-service',
        target_topic: 'simulator_events',
      }),
    });

    if (!response.ok) {
      const error = await response.json();
      console.error('[Simulator] Error response:', error);
      broadcast('simulator_status', {
        scenario: payload.scenario,
        status: 'error',
        error: error.error || 'Failed to trigger scenario'
      });
      return;
    }

    const result = await response.json();
    console.log('[Simulator] Scenario started:', result);

    // Notify all clients that scenario started
    broadcast('simulator_status', {
      scenario: payload.scenario,
      status: 'started',
      description: result.description,
      duration_seconds: result.duration_seconds
    });
  } catch (error) {
    console.error('[Simulator] Failed to trigger scenario:', error.message);
    broadcast('simulator_status', {
      scenario: payload.scenario,
      status: 'error',
      error: error.message
    });
  }
}

async function handleSimulatorStop() {
  console.log('[Simulator] Stopping all scenarios');

  try {
    const SIMULATOR_URL = process.env.SIMULATOR_URL || 'http://localhost:5001';
    const response = await fetch(`${SIMULATOR_URL}/simulator/stop`, {
      method: 'POST',
    });

    if (!response.ok) {
      console.error('[Simulator] Failed to stop');
      return;
    }

    const result = await response.json();
    console.log('[Simulator] Stopped:', result);

    broadcast('simulator_status', {
      status: 'stopped',
      message: 'All scenarios stopped'
    });
  } catch (error) {
    console.error('[Simulator] Failed to stop:', error.message);
  }
}

async function handleProducerStart(payload) {
  console.log('[Producer] Starting free producer:', payload);

  try {
    const SIMULATOR_URL = process.env.SIMULATOR_URL || 'http://localhost:5001';
    const response = await fetch(`${SIMULATOR_URL}/simulator/free-producer/start`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        throughput: payload.throughput || 5,
        consumer_group: 'checkout-service',
        target_topic: 'simulator_events',
      }),
    });

    if (!response.ok) {
      console.error('[Producer] Failed to start');
      return;
    }

    const result = await response.json();
    console.log('[Producer] Started:', result);

    broadcast('producer_status', {
      status: 'started',
      throughput: result.throughput
    });
  } catch (error) {
    console.error('[Producer] Failed to start:', error.message);
  }
}

async function handleProducerStop() {
  console.log('[Producer] Stopping free producer');

  try {
    const SIMULATOR_URL = process.env.SIMULATOR_URL || 'http://localhost:5001';
    const response = await fetch(`${SIMULATOR_URL}/simulator/free-producer/stop`, {
      method: 'POST',
    });

    if (!response.ok) {
      console.error('[Producer] Failed to stop');
      return;
    }

    const result = await response.json();
    console.log('[Producer] Stopped:', result);

    broadcast('producer_status', {
      status: 'stopped'
    });
  } catch (error) {
    console.error('[Producer] Failed to stop:', error.message);
  }
}

async function handleFeedback(payload) {
  await producer.send({
    topic: 'alert_feedback',
    messages: [
      {
        key: payload.alert_id,
        value: JSON.stringify({
          alert_id: payload.alert_id,
          feedback: payload.feedback,
          timestamp: new Date().toISOString(),
        }),
      },
    ],
  });
}

async function handleMetricsRequest(payload, ws) {
  try {
    // Force mock data due to Metrics API format issues
    const USE_MOCK_DATA = true;
    const metricsEnabled = !USE_MOCK_DATA && process.env.CONFLUENT_CLOUD_API_KEY && process.env.CONFLUENT_CLOUD_API_SECRET;

    if (!metricsEnabled) {
      // Use mock data if no API keys configured
      const mockMetrics = TOPICS.map((topic, idx) => {
        const baseRate = (idx + 1) * 100;
        return {
          topic,
          received_bytes: baseRate * 1024,
          sent_bytes: baseRate * 900,
          received_records: baseRate * 10,
          sent_records: baseRate * 9,
          retained_bytes: baseRate * 1024 * 1024 * (idx + 1),
          timeseries: {
            received_bytes: Array.from({ length: 15 }, (_, i) => ({
              timestamp: new Date(Date.now() - (14 - i) * 60000).toISOString(),
              value: baseRate * 1024 * (0.8 + Math.random() * 0.4),
            })),
            sent_bytes: Array.from({ length: 15 }, (_, i) => ({
              timestamp: new Date(Date.now() - (14 - i) * 60000).toISOString(),
              value: baseRate * 900 * (0.8 + Math.random() * 0.4),
            })),
          },
          timestamp: new Date().toISOString(),
        };
      });

      ws.send(JSON.stringify({
        type: 'metrics_response',
        data: mockMetrics,
        timestamp: new Date().toISOString(),
      }));
      return;
    }

    let metrics;

    if (payload.type === 'topic' && payload.topic) {
      // Get metrics for specific topic
      metrics = await metricsClient.getTopicMetrics(payload.topic);
    } else if (payload.type === 'cluster') {
      // Get cluster-level metrics
      metrics = await metricsClient.getClusterMetrics();
    } else if (payload.type === 'all_topics') {
      // Get metrics for all topics
      metrics = await metricsClient.getAllTopicsMetrics(TOPICS);
    }

    if (metrics) {
      ws.send(JSON.stringify({
        type: 'metrics_response',
        data: metrics,
        timestamp: new Date().toISOString(),
      }));
    }
  } catch (error) {
    console.error('Error handling metrics request:', error);
    ws.send(JSON.stringify({
      type: 'metrics_error',
      error: error.message,
    }));
  }
}

function broadcast(topic, message) {
  const payload = JSON.stringify({
    topic,
    data: message,
    timestamp: new Date().toISOString(),
  });

  clients.forEach((client) => {
    if (client.readyState === 1) { // OPEN
      client.send(payload);
    }
  });
}

async function run() {
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({
    topics: [
      'metrics_flattened',
      'velocity_anomaly_alerts',
      'enriched_alerts',
      'simulator_events',
    ],
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value ? JSON.parse(message.value.toString()) : null;

      console.log(`[${topic}] ${message.key?.toString()}`);

      const broadcastData = {
        key: message.key?.toString(),
        value,
        partition,
        offset: message.offset,
      };

      broadcast(topic, broadcastData);

      // Debug logging for simulator events
      if (topic === 'simulator_events') {
        console.log(`Broadcasting simulator event to ${clients.size} clients`);
      }
    },
  });

  console.log(`WebSocket server listening on port ${process.env.WEBSOCKET_PORT || 8080}`);

  // Only enable Metrics API if Cloud API credentials are provided
  const ENABLE_REAL_METRICS = false; // Disabled - API has format issues, using mock data

  if (process.env.CONFLUENT_CLOUD_API_KEY && process.env.CONFLUENT_CLOUD_API_SECRET && ENABLE_REAL_METRICS) {
    console.log('✅ Metrics API enabled - will fetch topic metrics every 60s');

    // Periodically fetch and broadcast topic metrics (every 60 seconds)
    setInterval(async () => {
      try {
        const allMetrics = await metricsClient.getAllTopicsMetrics(TOPICS);
        broadcast('topic_metrics', allMetrics);
      } catch (error) {
        console.error('Error fetching periodic metrics:', error);
      }
    }, 60000); // 1 minute

    // Fetch cluster metrics less frequently (every 2 minutes)
    setInterval(async () => {
      try {
        const clusterMetrics = await metricsClient.getClusterMetrics();
        broadcast('cluster_metrics', clusterMetrics);
      } catch (error) {
        console.error('Error fetching cluster metrics:', error);
      }
    }, 120000); // 2 minutes
  } else {
    console.log('⚠️  Metrics API disabled - set CONFLUENT_CLOUD_API_KEY and CONFLUENT_CLOUD_API_SECRET to enable topic metrics');
  }
}

run().catch(console.error);
