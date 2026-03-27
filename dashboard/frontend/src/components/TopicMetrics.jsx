import { useState, useEffect, memo, useMemo } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import './TopicMetrics.css';

const TopicMetrics = memo(function TopicMetrics({ ws }) {
  const [topicMetrics, setTopicMetrics] = useState([]);
  const [clusterMetrics, setClusterMetrics] = useState(null);
  const [selectedTopic, setSelectedTopic] = useState(null);
  const [loading, setLoading] = useState(true);
  const [isMockData, setIsMockData] = useState(false);
  const [errorMessage, setErrorMessage] = useState(null);

  useEffect(() => {
    if (!ws) {
      console.log('[TopicMetrics] No WebSocket connection');
      return;
    }

    const handleMessage = (event) => {
      const message = JSON.parse(event.data);

      if (message.topic === 'topic_metrics') {
        console.log('[TopicMetrics] Received topic_metrics broadcast');
        setTopicMetrics(message.data.value || []);
        setLoading(false);
      }

      if (message.topic === 'cluster_metrics') {
        console.log('[TopicMetrics] Received cluster_metrics broadcast');
        setClusterMetrics(message.data.value);
      }

      if (message.type === 'metrics_response') {
        console.log('[TopicMetrics] Received metrics_response', {
          isArray: Array.isArray(message.data),
          length: message.data?.length,
          mock: message.mock
        });
        if (Array.isArray(message.data)) {
          setTopicMetrics(message.data);
          setIsMockData(message.mock || false);
        } else if (message.data?.cluster_id) {
          setClusterMetrics(message.data);
        }
        setLoading(false);
      }

      if (message.type === 'metrics_error') {
        console.error('[TopicMetrics] Metrics error:', message.error);
        setErrorMessage(message.error);
        setLoading(false);
      }
    };

    ws.addEventListener('message', handleMessage);

    // Request initial metrics - wait for WebSocket to be ready
    const requestMetrics = () => {
      if (ws.readyState === WebSocket.OPEN) {
        console.log('[TopicMetrics] Requesting metrics...');
        ws.send(JSON.stringify({
          type: 'request_metrics',
          payload: { type: 'all_topics' },
        }));
      } else {
        console.log('[TopicMetrics] WebSocket not ready, state:', ws.readyState);
        // Try again in 500ms if not ready
        setTimeout(requestMetrics, 500);
      }
    };

    requestMetrics();

    return () => {
      ws.removeEventListener('message', handleMessage);
    };
  }, [ws]);

  const formatBytes = (bytes) => {
    if (bytes === 0) return '0 B';
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
    return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`;
  };

  const formatRate = (value) => {
    if (value === 0) return '0';
    if (value < 1000) return value.toFixed(1);
    if (value < 1000000) return `${(value / 1000).toFixed(1)}K`;
    return `${(value / 1000000).toFixed(1)}M`;
  };

  const lastUpdate = useMemo(() => {
    if (topicMetrics.length === 0) return null;
    return new Date(topicMetrics[0].timestamp);
  }, [topicMetrics]);

  const chartData = useMemo(() => topicMetrics.map(metric => ({
    name: metric.topic,
    'Received (KB/s)': (metric.received_bytes / 1024).toFixed(2),
    'Sent (KB/s)': (metric.sent_bytes / 1024).toFixed(2),
    'Records In': metric.received_records,
    'Records Out': metric.sent_records,
  })), [topicMetrics]);

  if (loading) {
    return (
      <div className="topic-metrics">
        <h2>Topic Metrics</h2>
        <div className="loading">
          <div className="spinner"></div>
          <p>Loading metrics from Confluent Cloud...</p>
        </div>
      </div>
    );
  }

  if (errorMessage) {
    return (
      <div className="topic-metrics">
        <h2>Topic Metrics</h2>
        <div className="error-state">
          <div className="error-icon">⚠️</div>
          <h3>Error Loading Metrics</h3>
          <p>{errorMessage}</p>
        </div>
      </div>
    );
  }

  if (topicMetrics.length === 0) {
    return (
      <div className="topic-metrics">
        <h2>Topic Metrics</h2>
        <div className="empty-state">
          <div className="empty-icon">📊</div>
          <h3>No Topic Metrics Available</h3>
          <p>Waiting for metrics data from Confluent Cloud Metrics API...</p>
          <div className="setup-instructions">
            <h4>To enable Topic Metrics:</h4>
            <ol>
              <li>Create Cloud API Keys at <a href="https://confluent.cloud/settings/api-keys" target="_blank" rel="noopener noreferrer">Confluent Cloud Console</a></li>
              <li>Add to <code>dashboard/backend/.env</code>:
                <pre>CONFLUENT_CLOUD_API_KEY=your-key{'\n'}CONFLUENT_CLOUD_API_SECRET=your-secret</pre>
              </li>
              <li>Restart backend: <code>make dev</code></li>
            </ol>
            <p>See <code>dashboard/TOPIC_METRICS.md</code> for details.</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="topic-metrics">
      <div className="metrics-header">
        <div className="metrics-header-top">
          <h2>Topic Metrics</h2>
          {lastUpdate && (
            <span className="last-update">
              Last update: {lastUpdate.toLocaleTimeString()}
            </span>
          )}
        </div>
        <span className="metrics-subtitle">
          Confluent Cloud Metrics API
        </span>
      </div>

      {clusterMetrics && (
        <div className="cluster-summary">
          <div className="cluster-stat">
            <span className="stat-label">Cluster Throughput</span>
            <span className="stat-value">{formatBytes(clusterMetrics.received_bytes + clusterMetrics.sent_bytes)}/s</span>
          </div>
          <div className="cluster-stat">
            <span className="stat-label">Active Connections</span>
            <span className="stat-value">{clusterMetrics.active_connections}</span>
          </div>
          <div className="cluster-stat">
            <span className="stat-label">Cluster ID</span>
            <span className="stat-value-small">{clusterMetrics.cluster_id}</span>
          </div>
        </div>
      )}

      <div className="topics-grid">
        {topicMetrics.map((metric, idx) => (
          <div
            key={idx}
            className={`topic-card ${selectedTopic === metric.topic ? 'selected' : ''}`}
            onClick={() => setSelectedTopic(selectedTopic === metric.topic ? null : metric.topic)}
          >
            <div className="topic-card-header">
              <h3>{metric.topic}</h3>
              <span className="topic-size">{formatBytes(metric.retained_bytes)}</span>
            </div>

            <div className="topic-card-stats">
              <div className="topic-stat">
                <span className="topic-stat-label">📥 Received</span>
                <div className="topic-stat-values">
                  <span className="topic-stat-value">{formatBytes(metric.received_bytes)}/s</span>
                  <span className="topic-stat-records">{formatRate(metric.received_records)} msg/s</span>
                </div>
              </div>

              <div className="topic-stat">
                <span className="topic-stat-label">📤 Sent</span>
                <div className="topic-stat-values">
                  <span className="topic-stat-value">{formatBytes(metric.sent_bytes)}/s</span>
                  <span className="topic-stat-records">{formatRate(metric.sent_records)} msg/s</span>
                </div>
              </div>
            </div>

            {selectedTopic === metric.topic && metric.timeseries?.received_bytes && (
              <div className="topic-timeseries">
                <div className="mini-chart">
                  <span className="chart-label">Last 15 min</span>
                  {metric.timeseries.received_bytes.slice(-15).map((point, i) => (
                    <div
                      key={i}
                      className="chart-bar"
                      style={{
                        height: `${Math.min(100, (point.value / Math.max(...metric.timeseries.received_bytes.map(p => p.value)) * 100))}%`
                      }}
                    />
                  ))}
                </div>
              </div>
            )}
          </div>
        ))}
      </div>

      {chartData.length > 0 && (
        <div className="metrics-chart">
          <h3>Topic Throughput Comparison</h3>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
              <XAxis
                dataKey="name"
                stroke="#9ca3af"
                style={{ fontSize: '0.75rem' }}
                angle={-45}
                textAnchor="end"
                height={80}
              />
              <YAxis
                stroke="#9ca3af"
                style={{ fontSize: '0.75rem' }}
              />
              <Tooltip
                contentStyle={{
                  background: 'rgba(0, 0, 0, 0.8)',
                  border: '1px solid rgba(255,255,255,0.2)',
                  borderRadius: '8px',
                  color: '#e4e7eb'
                }}
              />
              <Legend />
              <Bar dataKey="Received (KB/s)" fill="#10b981" />
              <Bar dataKey="Sent (KB/s)" fill="#3b82f6" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
});

export default TopicMetrics;
