import fetch from 'node-fetch';

/**
 * Confluent Cloud Metrics API Client
 * https://docs.confluent.io/cloud/current/monitoring/metrics-api.html
 */
export class MetricsAPIClient {
  constructor(apiKey, apiSecret, clusterId) {
    this.clusterId = clusterId;
    const credentials = Buffer.from(`${apiKey}:${apiSecret}`).toString('base64');
    this.headers = {
      'Authorization': `Basic ${credentials}`,
      'Content-Type': 'application/json',
    };
    this.baseUrl = 'https://api.telemetry.confluent.cloud/v2/metrics';
  }

  /**
   * Query metrics from Confluent Cloud
   * @param {string} metric - Metric name (e.g., 'io.confluent.kafka.server/received_bytes')
   * @param {Object} filters - Filters to apply (e.g., { topic: 'my-topic' })
   * @param {string} granularity - Time granularity (PT1M, PT5M, PT15M, PT1H, P1D)
   * @param {string} interval - Time interval (e.g., 'PT1H' for last hour)
   */
  async query(metric, filters = {}, granularity = 'PT1M', interval = 'PT1H') {
    // Build filter list
    const filterList = [
      {
        field: 'resource.kafka.id',
        op: 'EQ',
        value: this.clusterId,
      }
    ];

    // Add additional filters
    for (const [key, value] of Object.entries(filters)) {
      filterList.push({
        field: key,
        op: 'EQ',
        value: value,
      });
    }

    const body = {
      aggregations: [
        {
          metric: metric,
        },
      ],
      filter: {
        op: 'AND',
        filters: filterList,
      },
      granularity: granularity,
      intervals: [interval],
      limit: 1000,
    };

    try {
      console.log('[MetricsAPI] Query request:', JSON.stringify(body, null, 2));

      const response = await fetch(`${this.baseUrl}/cloud/query`, {
        method: 'POST',
        headers: this.headers,
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        const error = await response.text();
        console.error('[MetricsAPI] Error response:', error);
        throw new Error(`Metrics API error: ${response.status} - ${error}`);
      }

      const data = await response.json();
      console.log('[MetricsAPI] Success response received');
      return data;
    } catch (error) {
      console.error('Error querying metrics:', error.message);
      throw error;
    }
  }

  /**
   * Get topic metrics
   */
  async getTopicMetrics(topicName) {
    const metrics = {};

    try {
      // Received bytes (producer to broker)
      const receivedBytes = await this.query(
        'io.confluent.kafka.server/received_bytes',
        { 'metric.topic': topicName },
        'PT1M',
        'PT15M'
      );

      // Sent bytes (broker to consumer)
      const sentBytes = await this.query(
        'io.confluent.kafka.server/sent_bytes',
        { 'metric.topic': topicName },
        'PT1M',
        'PT15M'
      );

      // Received records
      const receivedRecords = await this.query(
        'io.confluent.kafka.server/received_records',
        { 'metric.topic': topicName },
        'PT1M',
        'PT15M'
      );

      // Sent records
      const sentRecords = await this.query(
        'io.confluent.kafka.server/sent_records',
        { 'metric.topic': topicName },
        'PT1M',
        'PT15M'
      );

      // Retained bytes (current size)
      const retainedBytes = await this.query(
        'io.confluent.kafka.server/retained_bytes',
        { 'metric.topic': topicName },
        'PT1M',
        'PT15M'
      );

      return {
        topic: topicName,
        received_bytes: this.extractLatestValue(receivedBytes),
        sent_bytes: this.extractLatestValue(sentBytes),
        received_records: this.extractLatestValue(receivedRecords),
        sent_records: this.extractLatestValue(sentRecords),
        retained_bytes: this.extractLatestValue(retainedBytes),
        timeseries: {
          received_bytes: this.extractTimeseries(receivedBytes),
          sent_bytes: this.extractTimeseries(sentBytes),
          received_records: this.extractTimeseries(receivedRecords),
          sent_records: this.extractTimeseries(sentRecords),
        },
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      console.error(`Error fetching metrics for topic ${topicName}:`, error);
      return null;
    }
  }

  /**
   * Get cluster-level metrics
   */
  async getClusterMetrics() {
    try {
      const receivedBytes = await this.query(
        'io.confluent.kafka.server/received_bytes',
        {},
        'PT1M',
        'PT15M'
      );

      const sentBytes = await this.query(
        'io.confluent.kafka.server/sent_bytes',
        {},
        'PT1M',
        'PT15M'
      );

      const activeConnections = await this.query(
        'io.confluent.kafka.server/active_connection_count',
        {},
        'PT1M',
        'PT15M'
      );

      return {
        cluster_id: this.clusterId,
        received_bytes: this.extractLatestValue(receivedBytes),
        sent_bytes: this.extractLatestValue(sentBytes),
        active_connections: this.extractLatestValue(activeConnections),
        timeseries: {
          received_bytes: this.extractTimeseries(receivedBytes),
          sent_bytes: this.extractTimeseries(sentBytes),
          active_connections: this.extractTimeseries(activeConnections),
        },
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      console.error('Error fetching cluster metrics:', error);
      return null;
    }
  }

  /**
   * Extract latest value from metrics response
   */
  extractLatestValue(response) {
    if (!response?.data || response.data.length === 0) {
      return 0;
    }

    const latestDataPoint = response.data[response.data.length - 1];
    return latestDataPoint?.value || 0;
  }

  /**
   * Extract timeseries data from metrics response
   */
  extractTimeseries(response) {
    if (!response?.data) {
      return [];
    }

    return response.data.map((point) => ({
      timestamp: point.timestamp,
      value: point.value || 0,
    }));
  }

  /**
   * Get metrics for all topics
   */
  async getAllTopicsMetrics(topics) {
    const results = await Promise.all(
      topics.map((topic) => this.getTopicMetrics(topic))
    );

    return results.filter((r) => r !== null);
  }
}
