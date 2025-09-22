import React, { useState, useEffect, useRef } from 'react';

const EventStream = ({ events = [], eventStats = { totalEvents: 0, eventTypeCounters: {} } }) => {
  const [filter, setFilter] = useState('all');
  const [isPaused, setIsPaused] = useState(false);
  const [displayedEvents, setDisplayedEvents] = useState([]);
  const listRef = useRef(null);

  useEffect(() => {
    if (!isPaused) {
      setDisplayedEvents(events);
      // Auto-scroll to top when new events arrive
      if (listRef.current) {
        listRef.current.scrollTop = 0;
      }
    }
  }, [events, isPaused]);

  const getEventIcon = (eventType) => {
    switch (eventType) {
      case 'page_view':
        return '👁️';
      case 'purchase':
        return '💳';
      case 'add_to_cart':
        return '🛒';
      case 'search':
        return '🔍';
      case 'login':
        return '🔐';
      case 'logout':
        return '🚪';
      default:
        return '📋';
    }
  };

  const getEventColor = (eventType) => {
    switch (eventType) {
      case 'purchase':
        return 'event-purchase';
      case 'add_to_cart':
        return 'event-cart';
      case 'page_view':
        return 'event-view';
      case 'search':
        return 'event-search';
      case 'login':
      case 'logout':
        return 'event-auth';
      default:
        return 'event-default';
    }
  };

  const isSuspiciousEvent = (event) => {
    return event.metadata && event.metadata.suspicious === true;
  };

  const formatTimestamp = (timestamp) => {
    return new Date(timestamp).toLocaleTimeString('en-US', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  const formatValue = (value) => {
    if (typeof value === 'number') {
      return value.toLocaleString();
    }
    return value;
  };

  const filteredEvents = displayedEvents.filter(event => {
    if (filter === 'all') return true;
    if (filter === 'suspicious') return isSuspiciousEvent(event);
    return event.event_type === filter;
  });

  const eventTypes = ['all', 'purchase', 'add_to_cart', 'page_view', 'search', 'suspicious'];

  const getFilterLabel = (type) => {
    switch (type) {
      case 'all':
        return '📊 All Events';
      case 'purchase':
        return '💳 Purchases';
      case 'add_to_cart':
        return '🛒 Add to Cart';
      case 'page_view':
        return '👁️ Page Views';
      case 'search':
        return '🔍 Searches';
      case 'suspicious':
        return '🚨 Suspicious';
      default:
        return type;
    }
  };

  // Use cumulative stats from props instead of calculating from buffer
  const { totalEvents, eventTypeCounters } = eventStats;

  return (
    <div>
      <div className="component-header">
        <span className="icon">📊</span>
        <h2>Live Event Stream</h2>
        <div className="stream-controls">
          <button
            className={`btn btn-sm ${isPaused ? 'btn-success' : 'btn-warning'}`}
            onClick={() => setIsPaused(!isPaused)}
            title={isPaused ? 'Resume stream' : 'Pause stream'}
          >
            {isPaused ? '▶️ Resume' : '⏸️ Pause'}
          </button>
        </div>
      </div>

      <div className="event-stream">
        
        {/* Event Stats */}
        <div className="event-stats">
          <div className="stats-row">
            <div className="stat-item">
              <span className="stat-label">Total Events:</span>
              <span className="stat-value">{totalEvents}</span>
            </div>
            <div className="stat-item">
              <span className="stat-label">Purchases:</span>
              <span className="stat-value">{eventTypeCounters.purchase || 0}</span>
            </div>
            <div className="stat-item">
              <span className="stat-label">Cart Adds:</span>
              <span className="stat-value">{eventTypeCounters.add_to_cart || 0}</span>
            </div>
            <div className="stat-item">
              <span className="stat-label">Page Views:</span>
              <span className="stat-value">{eventTypeCounters.page_view || 0}</span>
            </div>
          </div>
        </div>

        {/* Filter Controls */}
        <div className="filter-controls">
          <div className="filter-buttons">
            {eventTypes.map(type => (
              <button
                key={type}
                className={`btn btn-sm filter-btn ${filter === type ? 'active' : ''}`}
                onClick={() => setFilter(type)}
              >
                {getFilterLabel(type)}
                {type === 'all' ? ` (${totalEvents})` : 
                 type === 'suspicious' ? ` (${displayedEvents.filter(isSuspiciousEvent).length})` :
                 ` (${eventTypeCounters[type] || 0})`}
              </button>
            ))}
          </div>
        </div>

        {/* Events List */}
        <div className="events-container">
          {filteredEvents.length === 0 ? (
            <div className="no-events">
              <div className="no-events-icon">📭</div>
              <div className="no-events-text">
                {filter === 'all' ? 'No events yet' : `No ${filter} events`}
              </div>
              {isPaused && (
                <div className="no-events-subtext">
                  Stream is paused. Click Resume to see new events.
                </div>
              )}
            </div>
          ) : (
            <div ref={listRef} className="events-list">
              {filteredEvents.map((event, index) => (
                <div
                  key={`${event.event_id}-${index}`}
                  className={`event-item ${getEventColor(event.event_type)} ${
                    isSuspiciousEvent(event) ? 'suspicious' : ''
                  }`}
                >
                  <div className="event-header">
                    <div className="event-type-info">
                      <span className="event-icon">
                        {getEventIcon(event.event_type)}
                      </span>
                      <span className="event-type">
                        {event.event_type && typeof event.event_type === 'string' ? event.event_type.replace('_', ' ') : 'Unknown'}
                      </span>
                      {isSuspiciousEvent(event) && (
                        <span className="suspicious-badge">🚨 SUSPICIOUS</span>
                      )}
                    </div>
                    <span className="event-time">
                      {event.timestamp ? formatTimestamp(event.timestamp) : 'N/A'}
                    </span>
                  </div>

                  <div className="event-body">
                    <div className="event-details">
                      <div className="detail-group">
                        <span className="detail-label">User:</span>
                        <span className="detail-value">{event.user_id || 'Unknown'}</span>
                      </div>
                      <div className="detail-group">
                        <span className="detail-label">Session:</span>
                        <span className="detail-value">
                          {event.session_id && typeof event.session_id === 'string' ? event.session_id.substring(0, 8) + '...' : 'N/A'}
                        </span>
                      </div>
                      <div className="detail-group">
                        <span className="detail-label">Location:</span>
                        <span className="detail-value">{event.location || 'Unknown'}</span>
                      </div>
                      <div className="detail-group">
                        <span className="detail-label">Device:</span>
                        <span className="detail-value">{event.device_type || 'Unknown'}</span>
                      </div>
                    </div>

                    {/* Event-specific details */}
                    {event.product_name && (
                      <div className="product-info">
                        <div className="product-name">
                          📦 {event.product_name}
                        </div>
                        <div className="product-details">
                          {event.product_category && (
                            <span className="product-detail">
                              Category: {event.product_category}
                            </span>
                          )}
                          {event.product_price && (
                            <span className="product-detail">
                              Price: ${formatValue(event.product_price)}
                            </span>
                          )}
                          {event.quantity && event.quantity > 1 && (
                            <span className="product-detail">
                              Qty: {event.quantity}
                            </span>
                          )}
                        </div>
                      </div>
                    )}

                    {event.search_query && (
                      <div className="search-info">
                        <span className="search-query">"{event.search_query}"</span>
                        {event.search_results_count !== undefined && (
                          <span className="search-results">
                            ({formatValue(event.search_results_count)} results)
                          </span>
                        )}
                      </div>
                    )}

                    {event.page_url && (
                      <div className="page-info">
                        <span className="page-url">🔗 {event.page_url}</span>
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {filteredEvents.length > 0 && (
          <div className="events-footer">
            Showing {filteredEvents.length} of {displayedEvents.length} events
            {isPaused && <span className="pause-indicator"> (Paused)</span>}
          </div>
        )}

      </div>

      <style jsx>{`
        .stream-controls {
          margin-left: auto;
        }

        .event-stats {
          background: #f8f9fa;
          border-radius: 8px;
          padding: 1rem;
          margin-bottom: 1rem;
        }

        .stats-row {
          display: flex;
          justify-content: space-around;
          flex-wrap: wrap;
          gap: 1rem;
        }

        .stat-item {
          text-align: center;
        }

        .stat-label {
          display: block;
          font-size: 0.75rem;
          color: #6c757d;
          margin-bottom: 0.25rem;
        }

        .stat-value {
          display: block;
          font-size: 1.2rem;
          font-weight: 600;
          color: #495057;
        }

        .filter-controls {
          margin-bottom: 1rem;
        }

        .filter-buttons {
          display: flex;
          flex-wrap: wrap;
          gap: 0.5rem;
        }

        .filter-btn {
          background: #f8f9fa;
          color: #6c757d;
          border: 1px solid #e9ecef;
          transition: all 0.2s ease;
        }

        .filter-btn:hover {
          background: #e9ecef;
        }

        .filter-btn.active {
          background: #007bff;
          color: white;
          border-color: #007bff;
        }

        .events-container {
          min-height: 400px;
          max-height: 500px;
        }

        .no-events {
          text-align: center;
          padding: 3rem 1rem;
          color: #6c757d;
        }

        .no-events-icon {
          font-size: 3rem;
          margin-bottom: 1rem;
        }

        .no-events-text {
          font-size: 1.1rem;
          margin-bottom: 0.5rem;
        }

        .no-events-subtext {
          font-size: 0.875rem;
          color: #adb5bd;
        }

        .events-list {
          max-height: 500px;
          overflow-y: auto;
        }

        .event-item {
          border: 1px solid #e9ecef;
          border-radius: 8px;
          margin-bottom: 0.75rem;
          background: white;
          transition: all 0.2s ease;
        }

        .event-item:hover {
          transform: translateY(-1px);
          box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        }

        .event-item.suspicious {
          border-color: #dc3545;
          background: #fff5f5;
        }

        .event-purchase {
          border-left: 4px solid #28a745;
        }

        .event-cart {
          border-left: 4px solid #ffc107;
        }

        .event-view {
          border-left: 4px solid #17a2b8;
        }

        .event-search {
          border-left: 4px solid #6f42c1;
        }

        .event-auth {
          border-left: 4px solid #fd7e14;
        }

        .event-default {
          border-left: 4px solid #6c757d;
        }

        .event-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          padding: 1rem 1rem 0.5rem;
        }

        .event-type-info {
          display: flex;
          align-items: center;
          gap: 0.75rem;
        }

        .event-icon {
          font-size: 1.2rem;
        }

        .event-type {
          font-weight: 600;
          color: #495057;
          text-transform: capitalize;
        }

        .suspicious-badge {
          background: #dc3545;
          color: white;
          padding: 0.25rem 0.5rem;
          border-radius: 12px;
          font-size: 0.75rem;
          font-weight: 600;
        }

        .event-time {
          font-size: 0.875rem;
          color: #6c757d;
          font-family: monospace;
        }

        .event-body {
          padding: 0 1rem 1rem;
        }

        .event-details {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
          gap: 0.75rem;
          margin-bottom: 0.75rem;
        }

        .detail-group {
          display: flex;
          flex-direction: column;
        }

        .detail-label {
          font-size: 0.75rem;
          color: #6c757d;
          margin-bottom: 0.25rem;
        }

        .detail-value {
          font-size: 0.875rem;
          color: #495057;
          font-weight: 500;
        }

        .product-info {
          background: #f8f9fa;
          border-radius: 6px;
          padding: 0.75rem;
          margin-bottom: 0.5rem;
        }

        .product-name {
          font-weight: 600;
          color: #495057;
          margin-bottom: 0.5rem;
        }

        .product-details {
          display: flex;
          flex-wrap: wrap;
          gap: 1rem;
        }

        .product-detail {
          font-size: 0.875rem;
          color: #6c757d;
        }

        .search-info {
          background: #f8f9fa;
          border-radius: 6px;
          padding: 0.75rem;
          margin-bottom: 0.5rem;
        }

        .search-query {
          font-weight: 600;
          color: #495057;
          margin-right: 0.5rem;
        }

        .search-results {
          color: #6c757d;
          font-size: 0.875rem;
        }

        .page-info {
          background: #f8f9fa;
          border-radius: 6px;
          padding: 0.75rem;
          margin-bottom: 0.5rem;
        }

        .page-url {
          font-size: 0.875rem;
          color: #6c757d;
          word-break: break-all;
        }

        .events-footer {
          text-align: center;
          padding: 1rem;
          color: #6c757d;
          font-size: 0.875rem;
          border-top: 1px solid #e9ecef;
          margin-top: 1rem;
        }

        .pause-indicator {
          color: #ffc107;
          font-weight: 600;
        }

        @media (max-width: 768px) {
          .stats-row {
            flex-direction: column;
            gap: 0.5rem;
          }
          
          .filter-buttons {
            flex-direction: column;
          }
          
          .event-details {
            grid-template-columns: 1fr;
          }
          
          .product-details {
            flex-direction: column;
            gap: 0.25rem;
          }
        }
      `}</style>
    </div>
  );
};

export default EventStream;