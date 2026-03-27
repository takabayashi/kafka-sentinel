import { useState, useEffect } from 'react';
import MetricsView from './components/MetricsView';
import AlertFeed from './components/AlertFeed';
import SimulatorPanel from './components/SimulatorPanel';
import TopicMetrics from './components/TopicMetrics';
import './App.css';

const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8080';

function App() {
  const [metrics, setMetrics] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [enrichedAlerts, setEnrichedAlerts] = useState([]);
  const [connectionStatus, setConnectionStatus] = useState('disconnected');
  const [ws, setWs] = useState(null);
  const [activeTab, setActiveTab] = useState(() => {
    try {
      return localStorage.getItem('activeMetricsTab') || 'metrics';
    } catch (e) {
      console.error('Error reading localStorage:', e);
      return 'metrics';
    }
  });

  const handleTabChange = (tab) => {
    setActiveTab(tab);
    try {
      localStorage.setItem('activeMetricsTab', tab);
    } catch (e) {
      console.error('Error writing to localStorage:', e);
    }
  };

  useEffect(() => {
    let reconnectTimeout;
    let reconnectAttempts = 0;
    const maxReconnectAttempts = 10;
    let currentWebSocket = null;

    const connect = () => {
      currentWebSocket = new WebSocket(WS_URL);

      currentWebSocket.onopen = () => {
        console.log('✅ Connected to WebSocket');
        setConnectionStatus('connected');
        reconnectAttempts = 0;
      };

      currentWebSocket.onclose = (event) => {
        console.log('WebSocket closed', { code: event.code, wasClean: event.wasClean });
        setConnectionStatus('disconnected');

        if (event.code === 1001 || event.code === 1000) {
          console.log('Normal WebSocket closure, not reconnecting');
          return;
        }

        if (reconnectAttempts < maxReconnectAttempts) {
          const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), 10000);
          reconnectAttempts++;
          console.log(`Reconnecting in ${delay}ms (attempt ${reconnectAttempts}/${maxReconnectAttempts})`);

          reconnectTimeout = setTimeout(() => {
            connect();
          }, delay);
        } else {
          console.error('Max reconnection attempts reached');
          setConnectionStatus('error');
        }
      };

      currentWebSocket.onerror = (error) => {
        console.error('WebSocket error:', error);
        setConnectionStatus('error');
      };

      currentWebSocket.onmessage = (event) => {
        const message = JSON.parse(event.data);

        if (message.topic === 'metrics_flattened') {
          setMetrics((prev) => {
            const updated = [...prev, { ...message.data.value, timestamp: message.timestamp }];
            return updated.slice(-100);
          });
        }

        if (message.topic === 'velocity_anomaly_alerts') {
          setAlerts((prev) => {
            const updated = [{ ...message.data.value, timestamp: message.timestamp }, ...prev];
            return updated.slice(0, 50);
          });
        }

        if (message.topic === 'enriched_alerts') {
          setEnrichedAlerts((prev) => {
            const updated = [{ ...message.data.value, timestamp: message.timestamp }, ...prev];
            return updated.slice(0, 50);
          });
        }

        if (message.topic === 'simulator_status') {
          const status = message.data;
          if (status.status === 'started') {
            console.log(`✅ Scenario '${status.scenario}' started - ${status.description}`);
          } else if (status.status === 'error') {
            console.error(`❌ Scenario '${status.scenario}' failed:`, status.error);
          }
        }
      };

      setWs(currentWebSocket);
    };

    connect();

    return () => {
      if (reconnectTimeout) {
        clearTimeout(reconnectTimeout);
      }
      if (currentWebSocket) {
        currentWebSocket.close(1000, 'Component unmounting');
      }
    };
  }, []);

  const sendMessage = (message) => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  };

  const handleSimulatorAction = (scenario) => {
    sendMessage({
      type: 'simulator_action',
      payload: { scenario },
    });
  };

  const handleFeedback = (alertId, feedback) => {
    sendMessage({
      type: 'alert_feedback',
      payload: { alert_id: alertId, feedback },
    });
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>Kafka Anomaly Detection Dashboard</h1>
        <div className="connection-status">
          <span className={`status-indicator ${connectionStatus}`}></span>
          <span>{connectionStatus}</span>
        </div>
      </header>

      <div className="dashboard-grid">
        <div className="panel metrics-panel">
          <div className="panel-tabs">
            <button
              className={`tab-button ${activeTab === 'metrics' ? 'active' : ''}`}
              onClick={() => handleTabChange('metrics')}
            >
              Consumer Metrics
            </button>
            <button
              className={`tab-button ${activeTab === 'topics' ? 'active' : ''}`}
              onClick={() => handleTabChange('topics')}
            >
              Topic Metrics
            </button>
          </div>

          <div className="panel-content">
            {activeTab === 'metrics' ? (
              <MetricsView metrics={metrics} />
            ) : (
              <TopicMetrics ws={ws} />
            )}
          </div>
        </div>

        <div className="panel alerts-panel">
          <AlertFeed
            alerts={alerts}
            enrichedAlerts={enrichedAlerts}
            onFeedback={handleFeedback}
          />
        </div>

        <div className="panel simulator-panel">
          <SimulatorPanel onAction={handleSimulatorAction} ws={ws} />
        </div>
      </div>
    </div>
  );
}

export default App;
