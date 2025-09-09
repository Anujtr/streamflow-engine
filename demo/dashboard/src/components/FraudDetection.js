import React, { useState } from 'react';

const FraudDetection = ({ alerts = [] }) => {
  const [selectedAlert, setSelectedAlert] = useState(null);

  const getSeverityColor = (severity) => {
    switch (severity) {
      case 'high':
        return 'alert-high';
      case 'medium':
        return 'alert-medium';
      case 'low':
        return 'alert-low';
      default:
        return 'alert-medium';
    }
  };

  const getSeverityIcon = (severity) => {
    switch (severity) {
      case 'high':
        return '🚨';
      case 'medium':
        return '⚠️';
      case 'low':
        return '⚡';
      default:
        return '🔍';
    }
  };

  const getRiskLevel = (riskScore) => {
    if (riskScore >= 0.8) return 'Very High';
    if (riskScore >= 0.6) return 'High';
    if (riskScore >= 0.4) return 'Medium';
    if (riskScore >= 0.2) return 'Low';
    return 'Very Low';
  };

  const formatTimestamp = (timestamp) => {
    return new Date(timestamp).toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  const AlertModal = ({ alert, onClose }) => (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h3>
            {getSeverityIcon(alert.severity)} Fraud Alert Details
          </h3>
          <button className="modal-close" onClick={onClose}>×</button>
        </div>
        
        <div className="modal-body">
          <div className="alert-details">
            <div className="detail-row">
              <span className="detail-label">Alert ID:</span>
              <span className="detail-value">{alert.alert_id}</span>
            </div>
            <div className="detail-row">
              <span className="detail-label">User ID:</span>
              <span className="detail-value">{alert.user_id}</span>
            </div>
            <div className="detail-row">
              <span className="detail-label">Risk Score:</span>
              <span className="detail-value">
                {(alert.risk_score * 100).toFixed(0)}% ({getRiskLevel(alert.risk_score)})
              </span>
            </div>
            <div className="detail-row">
              <span className="detail-label">Severity:</span>
              <span className={`detail-value severity-${alert.severity}`}>
                {alert.severity.toUpperCase()}
              </span>
            </div>
            <div className="detail-row">
              <span className="detail-label">Time:</span>
              <span className="detail-value">{formatTimestamp(alert.timestamp)}</span>
            </div>
          </div>

          <div className="alert-description">
            <h4>Description</h4>
            <p>{alert.description}</p>
          </div>

          {alert.events && alert.events.length > 0 && (
            <div className="related-events">
              <h4>Related Events ({alert.events.length})</h4>
              <div className="events-list">
                {alert.events.slice(0, 10).map((event, index) => (
                  <div key={index} className="event-item">
                    <div className="event-header">
                      <span className="event-type">{event.event_type}</span>
                      <span className="event-time">
                        {formatTimestamp(event.timestamp)}
                      </span>
                    </div>
                    <div className="event-details">
                      {event.product_name && (
                        <span className="event-detail">Product: {event.product_name}</span>
                      )}
                      {event.product_price && (
                        <span className="event-detail">
                          Price: ${event.product_price}
                        </span>
                      )}
                      <span className="event-detail">Location: {event.location}</span>
                      <span className="event-detail">Device: {event.device_type}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );

  return (
    <div>
      <div className="component-header">
        <span className="icon">🛡️</span>
        <h2>Fraud Detection</h2>
        <div className="alert-count">
          {alerts.length} alerts
        </div>
      </div>

      <div className="fraud-detection">
        {alerts.length === 0 ? (
          <div className="no-alerts">
            <div className="no-alerts-icon">✅</div>
            <div className="no-alerts-text">
              No fraud alerts detected
            </div>
            <div className="no-alerts-subtext">
              All systems operating normally
            </div>
          </div>
        ) : (
          <div className="alerts-list">
            <div className="alerts-header">
              <h3>Recent Alerts</h3>
              <div className="severity-legend">
                <span className="legend-item high">🚨 High</span>
                <span className="legend-item medium">⚠️ Medium</span>
                <span className="legend-item low">⚡ Low</span>
              </div>
            </div>
            
            <div className="list">
              {alerts.slice(0, 10).map((alert, index) => (
                <div 
                  key={index} 
                  className={`list-item alert-item ${getSeverityColor(alert.severity)}`}
                  onClick={() => setSelectedAlert(alert)}
                >
                  <div className="alert-summary">
                    <div className="alert-icon-severity">
                      <span className="alert-icon">
                        {getSeverityIcon(alert.severity)}
                      </span>
                      <span className={`severity severity-${alert.severity}`}>
                        {alert.severity}
                      </span>
                    </div>
                    
                    <div className="alert-content">
                      <div className="alert-title">
                        User: {alert.user_id}
                      </div>
                      <div className="alert-desc">
                        {alert.description}
                      </div>
                      <div className="alert-meta">
                        <span className="risk-score">
                          Risk: {(alert.risk_score * 100).toFixed(0)}%
                        </span>
                        <span className="alert-time">
                          {formatTimestamp(alert.timestamp)}
                        </span>
                      </div>
                    </div>
                    
                    <div className="alert-actions">
                      <button className="btn btn-primary btn-sm">
                        View Details
                      </button>
                    </div>
                  </div>
                </div>
              ))}
            </div>

            {alerts.length > 10 && (
              <div className="more-alerts">
                +{alerts.length - 10} more alerts...
              </div>
            )}
          </div>
        )}
      </div>

      {selectedAlert && (
        <AlertModal 
          alert={selectedAlert} 
          onClose={() => setSelectedAlert(null)} 
        />
      )}

      <style jsx>{`
        .alert-count {
          margin-left: auto;
          font-size: 0.75rem;
          color: #6c757d;
          background: #f8f9fa;
          padding: 0.25rem 0.5rem;
          border-radius: 4px;
        }

        .no-alerts {
          text-align: center;
          padding: 2rem;
          color: #6c757d;
        }

        .no-alerts-icon {
          font-size: 3rem;
          margin-bottom: 1rem;
        }

        .no-alerts-text {
          font-size: 1.1rem;
          font-weight: 600;
          margin-bottom: 0.5rem;
          color: #495057;
        }

        .no-alerts-subtext {
          font-size: 0.875rem;
        }

        .alerts-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 1rem;
        }

        .alerts-header h3 {
          margin: 0;
          font-size: 1.1rem;
          color: #495057;
        }

        .severity-legend {
          display: flex;
          gap: 0.75rem;
        }

        .legend-item {
          font-size: 0.75rem;
          padding: 0.25rem 0.5rem;
          border-radius: 12px;
          background: #f8f9fa;
        }

        .legend-item.high {
          background: #f8d7da;
          color: #721c24;
        }

        .legend-item.medium {
          background: #fff3cd;
          color: #856404;
        }

        .legend-item.low {
          background: #d1ecf1;
          color: #0c5460;
        }

        .alert-item {
          cursor: pointer;
          transition: all 0.2s ease;
        }

        .alert-item:hover {
          transform: translateX(4px);
        }

        .alert-summary {
          display: flex;
          align-items: center;
          gap: 1rem;
        }

        .alert-icon-severity {
          display: flex;
          flex-direction: column;
          align-items: center;
          min-width: 60px;
        }

        .alert-icon {
          font-size: 1.5rem;
          margin-bottom: 0.25rem;
        }

        .severity {
          font-size: 0.75rem;
          font-weight: 600;
          text-transform: uppercase;
        }

        .severity-high {
          color: #dc3545;
        }

        .severity-medium {
          color: #ffc107;
        }

        .severity-low {
          color: #17a2b8;
        }

        .alert-content {
          flex: 1;
        }

        .alert-title {
          font-weight: 600;
          color: #495057;
          margin-bottom: 0.25rem;
        }

        .alert-desc {
          font-size: 0.875rem;
          color: #6c757d;
          margin-bottom: 0.5rem;
          line-height: 1.3;
        }

        .alert-meta {
          display: flex;
          gap: 1rem;
          font-size: 0.75rem;
        }

        .risk-score {
          font-weight: 600;
          color: #dc3545;
        }

        .alert-time {
          color: #6c757d;
        }

        .alert-actions {
          min-width: 100px;
        }

        .btn-sm {
          padding: 0.5rem 1rem;
          font-size: 0.875rem;
        }

        .more-alerts {
          text-align: center;
          padding: 1rem;
          color: #6c757d;
          font-style: italic;
        }

        /* Modal Styles */
        .modal-overlay {
          position: fixed;
          top: 0;
          left: 0;
          right: 0;
          bottom: 0;
          background: rgba(0, 0, 0, 0.5);
          display: flex;
          align-items: center;
          justify-content: center;
          z-index: 1000;
        }

        .modal-content {
          background: white;
          border-radius: 12px;
          width: 90%;
          max-width: 600px;
          max-height: 80vh;
          overflow-y: auto;
          box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
        }

        .modal-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          padding: 1.5rem;
          border-bottom: 1px solid #e9ecef;
        }

        .modal-header h3 {
          margin: 0;
          color: #495057;
        }

        .modal-close {
          background: none;
          border: none;
          font-size: 1.5rem;
          cursor: pointer;
          padding: 0.5rem;
          border-radius: 4px;
          color: #6c757d;
        }

        .modal-close:hover {
          background: #f8f9fa;
        }

        .modal-body {
          padding: 1.5rem;
        }

        .alert-details {
          margin-bottom: 1.5rem;
        }

        .detail-row {
          display: flex;
          justify-content: space-between;
          padding: 0.5rem 0;
          border-bottom: 1px solid #f8f9fa;
        }

        .detail-label {
          font-weight: 600;
          color: #495057;
        }

        .detail-value {
          color: #6c757d;
        }

        .alert-description {
          margin-bottom: 1.5rem;
        }

        .alert-description h4,
        .related-events h4 {
          margin: 0 0 1rem 0;
          color: #495057;
          font-size: 1.1rem;
        }

        .alert-description p {
          margin: 0;
          line-height: 1.5;
          color: #6c757d;
        }

        .events-list {
          max-height: 300px;
          overflow-y: auto;
        }

        .event-item {
          padding: 1rem;
          border: 1px solid #e9ecef;
          border-radius: 8px;
          margin-bottom: 0.75rem;
          background: #f8f9fa;
        }

        .event-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 0.5rem;
        }

        .event-type {
          font-weight: 600;
          color: #495057;
          text-transform: capitalize;
        }

        .event-time {
          font-size: 0.875rem;
          color: #6c757d;
        }

        .event-details {
          display: flex;
          flex-wrap: wrap;
          gap: 1rem;
        }

        .event-detail {
          font-size: 0.875rem;
          color: #6c757d;
        }
      `}</style>
    </div>
  );
};

export default FraudDetection;