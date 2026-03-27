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

  useEffect(() => {
    if (!ws) return;

    const handleMessage = (event) => {
      const message = JSON.parse(event.data);

      if (message.topic === 'simulator_events') {
        const eventData = message.data.value;
        setLiveEvents((prev) => {
          const updated = [{
            ...eventData,
            timestamp: message.timestamp,
            partition: message.data.partition,
            offset: message.data.offset
          }, ...prev];
          return updated.slice(0, 100); // Keep last 100 events
        });

        setSimulatorStats(prev => ({
          totalEvents: prev.totalEvents + 1,
          lastEventTime: message.timestamp,
          eventsPerSecond: prev.eventsPerSecond // Will be calculated separately
        }));
      }

      if (message.topic === 'simulator_status') {
        console.log('Simulator status:', message.data);
      }
    };

    ws.addEventListener('message', handleMessage);
    return () => ws.removeEventListener('message', handleMessage);
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

  return (
    <div className="simulator-panel">
      <h2>Anomaly Simulator</h2>
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
          <span>Processing scenario...</span>
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
    </div>
  );
});

export default SimulatorPanel;
