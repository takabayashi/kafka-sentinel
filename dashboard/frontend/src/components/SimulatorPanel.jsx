import { useState, memo, useEffect } from 'react';
import './SimulatorPanel.css';

const SimulatorPanel = memo(function SimulatorPanel({ onAction, ws }) {
  const [activeScenario, setActiveScenario] = useState(null);
  const [cooldown, setCooldown] = useState(false);
  const [activeTab, setActiveTab] = useState('scenarios');
  const [liveEvents, setLiveEvents] = useState([]);
  const [simulatorStats, setSimulatorStats] = useState({
    totalEvents: 0,
    eventsPerSecond: 0,
    lastEventTime: null
  });
  const [producerRunning, setProducerRunning] = useState(false);
  const [producerThroughput, setProducerThroughput] = useState(5);

  useEffect(() => {
    if (!ws) {
      console.log('[SimulatorPanel] No WebSocket connection');
      return;
    }

    const handleMessage = (event) => {
      const message = JSON.parse(event.data);

      if (message.topic === 'simulator_events') {
        console.log('[SimulatorPanel] Received simulator event:', message.data.value);
        const eventData = message.data.value;
        setLiveEvents((prev) => {
          const updated = [{
            ...eventData,
            timestamp: message.timestamp,
            partition: message.data.partition,
            offset: message.data.offset
          }, ...prev];
          console.log('[SimulatorPanel] Updated liveEvents, now has', updated.length, 'events');
          return updated.slice(0, 100); // Keep last 100 events
        });

        setSimulatorStats(prev => {
          const newStats = {
            totalEvents: prev.totalEvents + 1,
            lastEventTime: message.timestamp,
            eventsPerSecond: prev.eventsPerSecond
          };
          console.log('[SimulatorPanel] Updated stats:', newStats);
          return newStats;
        });
      }

      if (message.topic === 'simulator_status') {
        console.log('Simulator status:', message.data);
      }

      if (message.topic === 'producer_status') {
        console.log('Producer status:', message.data);
        if (message.data.status === 'started') {
          setProducerRunning(true);
          if (message.data.throughput) {
            setProducerThroughput(message.data.throughput);
          }
        } else if (message.data.status === 'stopped') {
          setProducerRunning(false);
        }
      }
    };

    console.log('[SimulatorPanel] Setting up WebSocket listener');
    ws.addEventListener('message', handleMessage);
    return () => {
      console.log('[SimulatorPanel] Removing WebSocket listener');
      ws.removeEventListener('message', handleMessage);
    };
  }, [ws]);

  const scenarios = [
    {
      id: 'lag_spike',
      name: 'Lag Spike',
      description: 'Sudden increase in consumer lag',
      icon: '📈',
      color: '#ef4444'
    },
    {
      id: 'consumer_slow',
      name: 'Consumer Slow',
      description: 'Gradual decrease in consumer throughput',
      icon: '🐌',
      color: '#f59e0b'
    },
    {
      id: 'rebalance_storm',
      name: 'Rebalance Storm',
      description: 'Multiple consecutive rebalances',
      icon: '🌪️',
      color: '#8b5cf6'
    },
    {
      id: 'hot_partition',
      name: 'Hot Partition',
      description: 'Uneven partition distribution',
      icon: '🔥',
      color: '#ec4899'
    }
  ];

  const handleScenarioClick = (scenario) => {
    if (cooldown) return;

    setActiveScenario(scenario.id);
    onAction(scenario.id);

    // Cooldown to prevent spam
    setCooldown(true);
    setTimeout(() => {
      setActiveScenario(null);
      setCooldown(false);
    }, 5000);
  };

  const handleStopScenario = () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'simulator_stop'
      }));
    }
    setActiveScenario(null);
    setCooldown(false);
  };

  const handleStartProducer = () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'producer_start',
        payload: { throughput: producerThroughput }
      }));
      setProducerRunning(true);
    }
  };

  const handleStopProducer = () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'producer_stop'
      }));
      setProducerRunning(false);
    }
  };

  const handleClearEvents = () => {
    setLiveEvents([]);
    setSimulatorStats({
      totalEvents: 0,
      eventsPerSecond: 0,
      lastEventTime: null
    });
  };

  const formatTimestamp = (timestamp) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString();
  };

  return (
    <div className="simulator-panel">
      <div className="simulator-header">
        <h2>Anomaly Simulator</h2>
        <div className="simulator-tabs">
          <button
            className={`tab-btn ${activeTab === 'scenarios' ? 'active' : ''}`}
            onClick={() => setActiveTab('scenarios')}
          >
            Scenarios
          </button>
          <button
            className={`tab-btn ${activeTab === 'activity' ? 'active' : ''}`}
            onClick={() => setActiveTab('activity')}
          >
            Live Activity
            {liveEvents.length > 0 && <span className="event-badge">{liveEvents.length}</span>}
          </button>
        </div>
      </div>

      {activeTab === 'scenarios' && (
        <>
          <p className="simulator-description">
            Trigger synthetic anomalies to test the detection system
          </p>

          <div className="scenarios-grid">
        {scenarios.map((scenario) => (
          <button
            key={scenario.id}
            className={`scenario-btn ${activeScenario === scenario.id ? 'active' : ''} ${cooldown ? 'disabled' : ''}`}
            onClick={() => handleScenarioClick(scenario)}
            disabled={cooldown}
            style={{
              '--scenario-color': scenario.color
            }}
          >
            <span className="scenario-icon">{scenario.icon}</span>
            <div className="scenario-info">
              <h3>{scenario.name}</h3>
              <p>{scenario.description}</p>
            </div>
          </button>
        ))}
      </div>

      {cooldown && (
        <div className="cooldown-notice">
          <span className="cooldown-spinner"></span>
          <span>Scenario running: {activeScenario?.replace('_', ' ')}</span>
          <button
            className="stop-scenario-btn"
            onClick={handleStopScenario}
            title="Stop scenario"
          >
            ⏹ Stop
          </button>
        </div>
      )}

          <div className="simulator-info">
            <h3>How it works</h3>
            <ul>
              <li>Click a scenario button to inject an anomaly</li>
              <li>Alert should appear within ~30 seconds</li>
              <li>AI diagnosis will include root cause analysis</li>
              <li>Provide feedback with 👍 or 👎</li>
            </ul>
          </div>
        </>
      )}

      {activeTab === 'activity' && (
        <div className="live-activity">
          <div className="producer-controls">
            <div className="producer-status">
              <span className={`status-dot ${producerRunning ? 'running' : 'stopped'}`}></span>
              <span className="status-text">
                {producerRunning ? `Producer Running (${producerThroughput} events/sec)` : 'Producer Stopped'}
              </span>
            </div>
            <div className="control-buttons">
              {!producerRunning ? (
                <>
                  <input
                    type="number"
                    value={producerThroughput}
                    onChange={(e) => setProducerThroughput(Number(e.target.value))}
                    min="1"
                    max="100"
                    className="throughput-input"
                    placeholder="Events/sec"
                  />
                  <button onClick={handleStartProducer} className="start-btn">
                    ▶ Start Producer
                  </button>
                </>
              ) : (
                <button onClick={handleStopProducer} className="stop-btn">
                  ⏹ Stop Producer
                </button>
              )}
              <button onClick={handleClearEvents} className="clear-btn" title="Clear event feed">
                🗑️ Clear
              </button>
            </div>
          </div>

          <div className="activity-stats">
            <div className="stat-card">
              <span className="stat-label">Total Events</span>
              <span className="stat-value">{simulatorStats.totalEvents}</span>
            </div>
            <div className="stat-card">
              <span className="stat-label">Events in Feed</span>
              <span className="stat-value">{liveEvents.length}</span>
            </div>
            {simulatorStats.lastEventTime && (
              <div className="stat-card">
                <span className="stat-label">Last Event</span>
                <span className="stat-value-small">{formatTimestamp(simulatorStats.lastEventTime)}</span>
              </div>
            )}
          </div>

          <div className="events-feed">
            {liveEvents.length === 0 ? (
              <div className="no-events">
                <div className="empty-icon">📭</div>
                <p>No events yet. Click a scenario button to start generating events.</p>
              </div>
            ) : (
              liveEvents.map((event, idx) => (
                <div key={idx} className="event-item">
                  <div className="event-header">
                    <span className="event-time">{formatTimestamp(event.timestamp)}</span>
                    <span className="event-partition">P{event.partition} • #{event.offset}</span>
                  </div>
                  <div className="event-body">
                    {event.event_type && (
                      <div className="event-field">
                        <span className="field-label">Type:</span>
                        <span className="field-value">{event.event_type}</span>
                      </div>
                    )}
                    {event.user_id && (
                      <div className="event-field">
                        <span className="field-label">User:</span>
                        <span className="field-value">{event.user_id}</span>
                      </div>
                    )}
                    {event.order_id && (
                      <div className="event-field">
                        <span className="field-label">Order:</span>
                        <span className="field-value">{event.order_id}</span>
                      </div>
                    )}
                    {event.amount && (
                      <div className="event-field">
                        <span className="field-label">Amount:</span>
                        <span className="field-value">${event.amount.toFixed(2)}</span>
                      </div>
                    )}
                    {event.anomaly_marker && (
                      <div className="anomaly-marker">
                        🔴 Anomaly: {event.anomaly_marker}
                      </div>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
});

export default SimulatorPanel;
