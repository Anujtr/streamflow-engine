import React, { useState } from 'react';

const TrafficControls = ({ onPatternChange, currentPattern, onClearEvents }) => {
  const [isLoading, setIsLoading] = useState(false);

  const patterns = [
    {
      name: 'normal',
      label: 'Normal Traffic',
      description: 'Steady baseline traffic',
      icon: '📊',
      color: 'btn-primary'
    },
    {
      name: 'flash_sale',
      label: 'Flash Sale',
      description: 'High traffic burst',
      icon: '⚡',
      color: 'btn-warning'
    },
    {
      name: 'fraud_attack',
      label: 'Fraud Attack',
      description: 'Simulated fraud scenario',
      icon: '🚨',
      color: 'btn-danger'
    },
    {
      name: 'peak_hours',
      label: 'Peak Hours',
      description: 'Heavy sustained traffic',
      icon: '🔥',
      color: 'btn-success'
    }
  ];

  const startPattern = async (patternName) => {
    setIsLoading(true);
    try {
      // Clear existing events before starting new pattern
      if (onClearEvents) {
        onClearEvents();
      }
      
      const response = await fetch(`http://localhost:8000/start-pattern/${patternName}`, {
        method: 'POST',
      });
      
      if (response.ok) {
        const result = await response.json();
        onPatternChange(patternName);
        console.log('Pattern started:', result);
      } else {
        console.error('Failed to start pattern');
      }
    } catch (error) {
      console.error('Error starting pattern:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const stopPattern = async () => {
    setIsLoading(true);
    try {
      const response = await fetch('http://localhost:8000/stop-pattern', {
        method: 'POST',
      });
      
      if (response.ok) {
        const result = await response.json();
        onPatternChange(null);
        console.log('Pattern stopped:', result);
      } else {
        console.error('Failed to stop pattern');
      }
    } catch (error) {
      console.error('Error stopping pattern:', error);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div>
      <div className="component-header">
        <span className="icon">🎛️</span>
        <h2>Traffic Controls</h2>
      </div>

      <div className="traffic-controls">
        <p className="controls-description">
          Control the demo event simulator to generate different traffic patterns
        </p>

        <div className="pattern-buttons">
          {patterns.map((pattern) => (
            <button
              key={pattern.name}
              className={`btn ${pattern.color} pattern-btn`}
              onClick={() => startPattern(pattern.name)}
              disabled={isLoading || currentPattern === pattern.name}
              title={pattern.description}
            >
              <span className="pattern-icon">{pattern.icon}</span>
              <div className="pattern-info">
                <div className="pattern-label">{pattern.label}</div>
                <div className="pattern-desc">{pattern.description}</div>
              </div>
            </button>
          ))}
        </div>

        <div className="control-actions">
          {currentPattern && (
            <button
              className="btn btn-secondary stop-btn"
              onClick={stopPattern}
              disabled={isLoading}
            >
              ⏹️ Stop Current Pattern
            </button>
          )}
          
          {isLoading && (
            <div className="loading-indicator">
              <div className="spinner"></div>
              Processing...
            </div>
          )}
        </div>

        {currentPattern && (
          <div className="current-status">
            <h3>Current Pattern</h3>
            <div className="status-card">
              <span className="status-icon">
                {patterns.find(p => p.name === currentPattern)?.icon}
              </span>
              <div className="status-info">
                <div className="status-name">
                  {patterns.find(p => p.name === currentPattern)?.label}
                </div>
                <div className="status-desc">
                  {patterns.find(p => p.name === currentPattern)?.description}
                </div>
              </div>
            </div>
          </div>
        )}
      </div>

      <style jsx>{`
        .traffic-controls {
          text-align: center;
        }

        .controls-description {
          color: #6c757d;
          margin-bottom: 1.5rem;
          line-height: 1.5;
        }

        .pattern-buttons {
          display: grid;
          gap: 1rem;
          margin-bottom: 1.5rem;
        }

        .pattern-btn {
          display: flex;
          align-items: center;
          justify-content: flex-start;
          text-align: left;
          padding: 1rem 1.5rem;
          transition: all 0.2s ease;
          border-radius: 12px;
        }

        .pattern-btn:disabled {
          opacity: 0.6;
          cursor: not-allowed;
          transform: none !important;
          box-shadow: none !important;
        }

        .pattern-icon {
          font-size: 1.5rem;
          margin-right: 1rem;
          min-width: 2rem;
        }

        .pattern-info {
          flex: 1;
        }

        .pattern-label {
          font-weight: 600;
          font-size: 1rem;
          margin-bottom: 0.25rem;
        }

        .pattern-desc {
          font-size: 0.875rem;
          opacity: 0.8;
          font-weight: 400;
        }

        .control-actions {
          margin-bottom: 1.5rem;
        }

        .stop-btn {
          width: 100%;
          padding: 0.75rem;
          font-size: 1rem;
          margin-bottom: 1rem;
        }

        .loading-indicator {
          display: flex;
          align-items: center;
          justify-content: center;
          color: #6c757d;
          font-size: 0.875rem;
        }

        .current-status h3 {
          margin: 0 0 1rem 0;
          color: #495057;
          font-size: 1.1rem;
          text-align: left;
        }

        .status-card {
          display: flex;
          align-items: center;
          background: #f8f9fa;
          border-radius: 8px;
          padding: 1rem;
          text-align: left;
        }

        .status-icon {
          font-size: 1.5rem;
          margin-right: 1rem;
        }

        .status-info {
          flex: 1;
        }

        .status-name {
          font-weight: 600;
          color: #495057;
          margin-bottom: 0.25rem;
        }

        .status-desc {
          font-size: 0.875rem;
          color: #6c757d;
        }
      `}</style>
    </div>
  );
};

export default TrafficControls;