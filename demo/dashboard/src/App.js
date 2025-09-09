import React, { useState, useEffect } from 'react';
import './App.css';
import SalesMetrics from './components/SalesMetrics';
import FraudDetection from './components/FraudDetection';
import EventStream from './components/EventStream';
import TrafficControls from './components/TrafficControls';

function App() {
  const [salesData, setSalesData] = useState(null);
  const [fraudAlerts, setFraudAlerts] = useState([]);
  const [recentEvents, setRecentEvents] = useState([]);
  const [isConnected, setIsConnected] = useState(false);
  const [currentPattern, setCurrentPattern] = useState(null);

  useEffect(() => {
    // Connect to both WebSocket endpoints
    const simulatorWs = new WebSocket('ws://localhost:8000/ws');
    const pipelinesWs = new WebSocket('ws://localhost:8081/ws');

    // Simulator WebSocket (for events)
    simulatorWs.onopen = () => {
      console.log('Connected to simulator WebSocket');
      setIsConnected(true);
    };

    simulatorWs.onmessage = (event) => {
      const message = JSON.parse(event.data);
      
      if (message.type === 'event') {
        setRecentEvents(prev => {
          const newEvents = [message.data, ...prev.slice(0, 49)]; // Keep last 50 events
          return newEvents;
        });
      } else if (message.type === 'stats') {
        // Additional stats from simulator
        console.log('Simulator stats:', message.data);
      }
    };

    simulatorWs.onerror = () => {
      console.error('Simulator WebSocket error');
      setIsConnected(false);
    };

    simulatorWs.onclose = () => {
      console.log('Simulator WebSocket closed');
      setIsConnected(false);
    };

    // Pipelines WebSocket (for analytics and fraud detection)
    pipelinesWs.onopen = () => {
      console.log('Connected to pipelines WebSocket');
    };

    pipelinesWs.onmessage = (event) => {
      const message = JSON.parse(event.data);
      
      if (message.type === 'sales_update') {
        setSalesData(message.data);
      } else if (message.type === 'fraud_alert') {
        setFraudAlerts(prev => [message.data, ...prev.slice(0, 19)]); // Keep last 20 alerts
      }
    };

    pipelinesWs.onerror = () => {
      console.error('Pipelines WebSocket error');
    };

    pipelinesWs.onclose = () => {
      console.log('Pipelines WebSocket closed');
    };

    // Cleanup
    return () => {
      simulatorWs.close();
      pipelinesWs.close();
    };
  }, []);

  // Fetch initial data
  useEffect(() => {
    const fetchData = async () => {
      try {
        // Get current sales data from pipelines
        const salesResponse = await fetch('http://localhost:8081/api/sales');
        if (salesResponse.ok) {
          const sales = await salesResponse.json();
          setSalesData(sales);
        }

        // Get current fraud alerts
        const fraudResponse = await fetch('http://localhost:8081/api/fraud');
        if (fraudResponse.ok) {
          const fraud = await fraudResponse.json();
          setFraudAlerts(fraud.alerts || []);
        }
      } catch (error) {
        console.error('Error fetching initial data:', error);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 5000); // Refresh every 5 seconds

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="App">
      <header className="App-header">
        <h1>🌊 StreamFlow Engine - Real-time Analytics Dashboard</h1>
        <div className="connection-status">
          <span className={`status-indicator ${isConnected ? 'connected' : 'disconnected'}`}>
            {isConnected ? '🟢 Connected' : '🔴 Disconnected'}
          </span>
          {currentPattern && (
            <span className="current-pattern">
              📊 Running: {currentPattern}
            </span>
          )}
        </div>
      </header>

      <div className="dashboard-grid">
        <div className="controls-section">
          <TrafficControls 
            onPatternChange={setCurrentPattern}
            currentPattern={currentPattern}
          />
        </div>

        <div className="metrics-section">
          <SalesMetrics data={salesData} />
        </div>

        <div className="fraud-section">
          <FraudDetection alerts={fraudAlerts} />
        </div>

        <div className="events-section">
          <EventStream events={recentEvents} />
        </div>
      </div>
    </div>
  );
}

export default App;