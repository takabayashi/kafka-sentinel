import { useMemo, memo } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import './MetricsView.css';

const MetricsView = memo(function MetricsView({ metrics }) {
  // Get last update time
  const lastUpdate = useMemo(() => {
    if (metrics.length === 0) return null;
    return new Date(metrics[metrics.length - 1].timestamp);
  }, [metrics]);

  // Memoize expensive calculations to prevent unnecessary re-renders
  const { groupedMetrics, latestMetrics, chartData } = useMemo(() => {
    // Group metrics by consumer group
    const grouped = metrics.reduce((acc, metric) => {
      const group = metric.consumer_group || 'unknown';
      if (!acc[group]) {
        acc[group] = [];
      }
      acc[group].push(metric);
      return acc;
    }, {});

    const consumerGroups = Object.keys(grouped);
    const latest = consumerGroups.map(group => {
      const groupMetrics = grouped[group];
      return groupMetrics[groupMetrics.length - 1];
    });

    // Prepare chart data - use latest 50 data points
    const chart = metrics.slice(-50).map(m => ({
      time: formatTimestamp(m.timestamp),
      lag: m.current_lag || 0,
      read_speed: m.read_speed_msg_per_sec || 0,
      write_speed: m.write_speed_msg_per_sec || 0,
    }));

    return { groupedMetrics: grouped, latestMetrics: latest, chartData: chart };
  }, [metrics]);

  return (
    <div className="metrics-view">
      <div className="metrics-view-header">
        <h2>Real-Time Metrics</h2>
        {lastUpdate && (
          <span className="last-update">
            Last update: {lastUpdate.toLocaleTimeString()}
          </span>
        )}
      </div>

      <div className="metrics-summary">
        {latestMetrics.map((metric, idx) => (
          <div key={idx} className="metric-card">
            <div className="metric-header">
              <h3>{metric.consumer_group}</h3>
              <span className="metric-topic">{metric.topic}</span>
            </div>
            <div className="metric-stats">
              <div className="stat">
                <span className="stat-label">Lag</span>
                <span className="stat-value">{formatNumber(metric.current_lag)}</span>
              </div>
              <div className="stat">
                <span className="stat-label">Read Speed</span>
                <span className="stat-value">{formatNumber(metric.read_speed_msg_per_sec)}/s</span>
              </div>
              <div className="stat">
                <span className="stat-label">Write Speed</span>
                <span className="stat-value">{formatNumber(metric.write_speed_msg_per_sec)}/s</span>
              </div>
              <div className="stat">
                <span className="stat-label">Time to Catch Up</span>
                <span className="stat-value">{metric.time_to_catch_up_seconds?.toFixed(1) || 0}s</span>
              </div>
            </div>
          </div>
        ))}
      </div>

      <div className="metrics-chart">
        <h3>Historical Trends</h3>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" />
            <XAxis
              dataKey="time"
              stroke="#9ca3af"
              style={{ fontSize: '0.75rem' }}
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
            <Line
              type="monotone"
              dataKey="lag"
              stroke="#ef4444"
              strokeWidth={2}
              dot={false}
              name="Lag"
            />
            <Line
              type="monotone"
              dataKey="read_speed"
              stroke="#10b981"
              strokeWidth={2}
              dot={false}
              name="Read Speed"
            />
            <Line
              type="monotone"
              dataKey="write_speed"
              stroke="#3b82f6"
              strokeWidth={2}
              dot={false}
              name="Write Speed"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

// Helper functions outside component to avoid recreating on every render
function formatTimestamp(timestamp) {
  const date = new Date(timestamp);
  return date.toLocaleTimeString();
}

function formatNumber(num) {
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
  return num?.toFixed(0) || 0;
}

export default MetricsView;
