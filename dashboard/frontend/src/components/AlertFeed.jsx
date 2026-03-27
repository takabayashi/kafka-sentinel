import { useState, memo, useMemo } from 'react';
import './AlertFeed.css';

const AlertFeed = memo(function AlertFeed({ alerts, enrichedAlerts, onFeedback }) {
  const [expandedAlert, setExpandedAlert] = useState(null);

  const lastUpdate = useMemo(() => {
    if (alerts.length === 0) return null;
    return new Date(alerts[0].timestamp);
  }, [alerts]);

  // Merge alerts with enriched data
  const mergedAlerts = alerts.map(alert => {
    const enriched = enrichedAlerts.find(e => e.alert_id === alert.alert_id);
    return { ...alert, ...enriched };
  });

  const getSeverityClass = (severity) => {
    if (!severity) return 'medium';
    if (severity.toLowerCase() === 'critical' || severity.toLowerCase() === 'high') return 'high';
    if (severity.toLowerCase() === 'low') return 'low';
    return 'medium';
  };

  const formatTimestamp = (timestamp) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diff = now - date;

    if (diff < 60000) return 'Just now';
    if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
    if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
    return date.toLocaleString();
  };

  return (
    <div className="alert-feed">
      <div className="alert-feed-header">
        <h2>Alert Feed</h2>
        {lastUpdate && (
          <span className="last-update">
            Last update: {lastUpdate.toLocaleTimeString()}
          </span>
        )}
      </div>

      <div className="alerts-list">
        {mergedAlerts.length === 0 ? (
          <div className="no-alerts">
            <p>No alerts yet. System is monitoring...</p>
          </div>
        ) : (
          mergedAlerts.map((alert, idx) => (
            <div
              key={idx}
              className={`alert-item ${getSeverityClass(alert.severity)} ${expandedAlert === idx ? 'expanded' : ''}`}
              onClick={() => setExpandedAlert(expandedAlert === idx ? null : idx)}
            >
              <div className="alert-header">
                <div className="alert-title">
                  <span className={`severity-badge ${getSeverityClass(alert.severity)}`}>
                    {alert.severity || 'ALERT'}
                  </span>
                  <span className="alert-type">{alert.anomaly_type || 'Anomaly Detected'}</span>
                </div>
                <span className="alert-time">{formatTimestamp(alert.timestamp)}</span>
              </div>

              <div className="alert-body">
                <div className="alert-group">
                  <strong>{alert.consumer_group}</strong>
                  {alert.topic && <span className="alert-topic"> on {alert.topic}</span>}
                </div>

                {alert.diagnosis && (
                  <div className="alert-diagnosis">
                    {alert.diagnosis}
                  </div>
                )}

                {alert.recommended_action && expandedAlert === idx && (
                  <div className="alert-recommendation">
                    <strong>Recommended Action:</strong>
                    <p>{alert.recommended_action}</p>
                  </div>
                )}

                {alert.metric_details && expandedAlert === idx && (
                  <div className="alert-details">
                    <strong>Details:</strong>
                    <pre>{JSON.stringify(alert.metric_details, null, 2)}</pre>
                  </div>
                )}
              </div>

              <div className="alert-actions">
                <button
                  className="feedback-btn thumbs-up"
                  onClick={(e) => {
                    e.stopPropagation();
                    onFeedback(alert.alert_id, 'up');
                  }}
                  title="Helpful"
                >
                  👍
                </button>
                <button
                  className="feedback-btn thumbs-down"
                  onClick={(e) => {
                    e.stopPropagation();
                    onFeedback(alert.alert_id, 'down');
                  }}
                  title="Not helpful"
                >
                  👎
                </button>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
});

export default AlertFeed;
