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
  
  // Product lookup for converting product IDs to names
  const deriveProductName = (productId) => {
    const productMap = {
      'prod_001': 'Wireless Headphones',
      'prod_002': 'Running Shoes', 
      'prod_003': 'Coffee Maker',
      'prod_004': 'Smartphone Case',
      'prod_005': 'Yoga Mat',
      'prod_006': 'LED Desk Lamp',
      'prod_007': 'Bluetooth Speaker',
      'prod_008': 'Kitchen Knife Set',
      'prod_009': 'Fitness Tracker',
      'prod_010': 'Laptop Stand'
    };
    return productMap[productId] || null;
  };
  
  // Product category lookup for converting product IDs to categories
  const deriveProductCategory = (productId) => {
    const categoryMap = {
      'prod_001': 'Electronics',
      'prod_002': 'Sports',
      'prod_003': 'Home',
      'prod_004': 'Electronics',
      'prod_005': 'Sports',
      'prod_006': 'Home',
      'prod_007': 'Electronics',
      'prod_008': 'Home',
      'prod_009': 'Sports',
      'prod_010': 'Electronics'
    };
    return categoryMap[productId] || null;
  };
  
  // Persistent event counters for continuous stats
  const [eventStats, setEventStats] = useState({
    totalEvents: 0,
    eventTypeCounters: {
      purchase: 0,
      add_to_cart: 0,
      page_view: 0,
      search: 0,
      product_view: 0,
      user_login: 0,
      user_logout: 0,
      user_registration: 0
    }
  });

  // Real-time sales analytics
  const [realTimeSales, setRealTimeSales] = useState({
    totalRevenue: 0,
    transactionCount: 0,
    averageOrderValue: 0,
    revenueTimeline: [],
    topProducts: {},
    salesByLocation: {},
    salesByCategory: {},
    recentPurchases: []
  });

  // Function to clear existing events and reset stats
  const clearEvents = () => {
    console.log('Clearing existing events and resetting stats');
    setRecentEvents([]);
    setEventStats({
      totalEvents: 0,
      eventTypeCounters: {
        purchase: 0,
        add_to_cart: 0,
        page_view: 0,
        search: 0,
        product_view: 0,
        user_login: 0,
        user_logout: 0,
        user_registration: 0
      }
    });
    setRealTimeSales({
      totalRevenue: 0,
      transactionCount: 0,
      averageOrderValue: 0,
      revenueTimeline: [],
      topProducts: {},
      salesByLocation: {},
      salesByCategory: {},
      recentPurchases: []
    });
  };

  useEffect(() => {
    // Get URLs from environment variables with fallbacks
    const simulatorUrl = process.env.REACT_APP_SIMULATOR_URL || 'http://localhost:8000';
    const pipelinesUrl = process.env.REACT_APP_PIPELINES_URL || 'http://localhost:8082';
    
    // Debug environment variables and connection URLs
    console.log('🔧 Dashboard WebSocket Configuration:', {
      simulatorUrl,
      pipelinesUrl,
      simulatorWsUrl: `ws://${simulatorUrl.replace('http://', '')}/ws`,
      pipelinesWsUrl: `ws://${pipelinesUrl.replace('http://', '')}/ws`
    });
    
    // Connect to both WebSocket endpoints
    const simulatorWsUrl = `ws://${simulatorUrl.replace('http://', '')}/ws`;
    const pipelinesWsUrl = `ws://${pipelinesUrl.replace('http://', '')}/ws`;
    
    let simulatorWs = null;
    let pipelinesWs = null;
    let reconnectAttempts = 0;
    const maxReconnectAttempts = 5;
    let reconnectTimeout = null;
    
    // Function to create simulator WebSocket with retry logic
    const createSimulatorWebSocket = () => {
      console.log('🔌 Attempting to connect to simulator WebSocket:', simulatorWsUrl, `(Attempt ${reconnectAttempts + 1}/${maxReconnectAttempts + 1})`);
      simulatorWs = new WebSocket(simulatorWsUrl);
      
      // Add connection timeout
      const connectionTimeout = setTimeout(() => {
        if (simulatorWs.readyState === WebSocket.CONNECTING) {
          console.error('⏰ Simulator WebSocket connection timeout');
          simulatorWs.close();
        }
      }, 10000); // 10 second timeout
      
      // Simulator WebSocket event handlers
      simulatorWs.onopen = () => {
        clearTimeout(connectionTimeout);
        console.log('✅ Connected to simulator WebSocket successfully');
        setIsConnected(true);
        reconnectAttempts = 0; // Reset retry counter on successful connection
      };
      
      simulatorWs.onmessage = (event) => {
      const message = JSON.parse(event.data);
      
      if (message.type === 'event') {
        setRecentEvents(prev => {
          try {
            // Debug logging to identify data format
            console.log('Raw message received:', message);
            console.log('Message data structure:', message.data);
            
            // Parse the actual event data from the nested JSON structure
            const eventData = JSON.parse(message.data.value);
            console.log('Parsed event data:', eventData);
            
            // Update cumulative event stats
            setEventStats(prevStats => ({
              totalEvents: prevStats.totalEvents + 1,
              eventTypeCounters: {
                ...prevStats.eventTypeCounters,
                [eventData.event_type]: (prevStats.eventTypeCounters[eventData.event_type] || 0) + 1
              }
            }));

            // DEBUG: Log purchase events in detail
            if (eventData.event_type === 'purchase') {
              console.log('🔍 PURCHASE EVENT DETECTED:', {
                event_type: eventData.event_type,
                metadata_total_amount: eventData.metadata?.total_amount,
                purchase_amount_used: eventData.event_type === 'purchase' ? eventData.metadata?.total_amount : eventData.product_price
              });
            }
            
            // Process purchase events for comprehensive real-time sales analytics
            // Purchase events have total_amount in metadata, not product_price
            const purchaseAmount = eventData.event_type === 'purchase' ? eventData.metadata?.total_amount : eventData.product_price;
            if (eventData.event_type === 'purchase' && purchaseAmount) {
              console.log('🛒 PROCESSING PURCHASE EVENT!', eventData, 'Amount:', purchaseAmount);
              setRealTimeSales(prevSales => {
                const newTotalRevenue = prevSales.totalRevenue + purchaseAmount;
                const newTransactionCount = prevSales.transactionCount + 1;
                const newAverageOrderValue = newTotalRevenue / newTransactionCount;

                // Update top products from purchased items in metadata
                const newTopProducts = { ...prevSales.topProducts };
                const purchasedItems = eventData.metadata?.items || [];
                
                if (purchasedItems.length > 0) {
                  purchasedItems.forEach(item => {
                    const productKey = item.product_id || 'Unknown Product';
                    // Use the product name from the main event data or derive from product_id
                    const productName = eventData.product_name || deriveProductName(item.product_id) || productKey;
                    if (!newTopProducts[productKey]) {
                      newTopProducts[productKey] = { 
                        product_name: productName, 
                        revenue: 0, 
                        units: 0,
                        category: eventData.product_category || 'Uncategorized'
                      };
                    }
                    newTopProducts[productKey].revenue += (item.total || 0);
                    newTopProducts[productKey].units += (item.quantity || 1);
                  });
                } else {
                  // Fallback for events without detailed items
                  const productKey = eventData.product_name || 'Unknown Product';
                  if (!newTopProducts[productKey]) {
                    newTopProducts[productKey] = { 
                      product_name: productKey, 
                      revenue: 0, 
                      units: 0,
                      category: eventData.product_category || 'Uncategorized'
                    };
                  }
                  newTopProducts[productKey].revenue += purchaseAmount;
                  newTopProducts[productKey].units += (eventData.quantity || 1);
                }

                // Update sales by location
                const newSalesByLocation = { ...prevSales.salesByLocation };
                const locationKey = eventData.location || 'Unknown';
                if (!newSalesByLocation[locationKey]) {
                  newSalesByLocation[locationKey] = { location: locationKey, revenue: 0, orders: 0 };
                }
                newSalesByLocation[locationKey].revenue += purchaseAmount;
                newSalesByLocation[locationKey].orders += 1;

                // Update sales by category - always use event's product_category
                const newSalesByCategory = { ...prevSales.salesByCategory };
                const categoryKey = eventData.product_category || deriveProductCategory(eventData.product_id) || 'Uncategorized';
                if (!newSalesByCategory[categoryKey]) {
                  newSalesByCategory[categoryKey] = { category: categoryKey, revenue: 0 };
                }
                newSalesByCategory[categoryKey].revenue += purchaseAmount;

                // Add to revenue timeline (keep last 20 data points)
                const now = new Date();
                const newTimelinePoint = {
                  timestamp: now.toLocaleTimeString('en-US', { 
                    hour: '2-digit', 
                    minute: '2-digit' 
                  }),
                  revenue: newTotalRevenue,
                  orders: newTransactionCount
                };
                const newTimeline = [...prevSales.revenueTimeline, newTimelinePoint].slice(-20);

                // Add to recent purchases (keep last 10)
                const newRecentPurchases = [...prevSales.recentPurchases, {
                  timestamp: eventData.timestamp,
                  revenue: purchaseAmount,
                  product_name: eventData.product_name || 'Multiple Items',
                  user_id: eventData.user_id,
                  location: eventData.location
                }].slice(-10);

                return {
                  totalRevenue: newTotalRevenue,
                  transactionCount: newTransactionCount,
                  averageOrderValue: newAverageOrderValue,
                  revenueTimeline: newTimeline,
                  topProducts: newTopProducts,
                  salesByLocation: newSalesByLocation,
                  salesByCategory: newSalesByCategory,
                  recentPurchases: newRecentPurchases
                };
              });
            }
            
            const newEvents = [eventData, ...prev.slice(0, 49)]; // Keep last 50 events
            return newEvents;
          } catch (error) {
            console.error('Error parsing event data:', error);
            console.error('Raw message that failed:', message);
            return prev;
          }
        });
      } else if (message.type === 'stats') {
        // Additional stats from simulator
        console.log('Simulator stats:', message.data);
      }
    };

      simulatorWs.onerror = (error) => {
        clearTimeout(connectionTimeout);
        console.error('❌ Simulator WebSocket error:', error);
        console.error('❌ Connection URL was:', simulatorWsUrl);
        console.error('❌ WebSocket ready state:', simulatorWs?.readyState);
        setIsConnected(false);
        scheduleReconnect();
      };

      simulatorWs.onclose = (event) => {
        clearTimeout(connectionTimeout);
        console.log('❌ Simulator WebSocket closed:', {
          code: event.code,
          reason: event.reason,
          wasClean: event.wasClean,
          url: simulatorWsUrl
        });
        setIsConnected(false);
        if (!event.wasClean) {
          scheduleReconnect();
        }
      };
    };
    
    // Function to schedule reconnection with exponential backoff
    const scheduleReconnect = () => {
      if (reconnectAttempts >= maxReconnectAttempts) {
        console.error('🚫 Max reconnection attempts reached. Giving up.');
        return;
      }
      
      const delay = Math.pow(2, reconnectAttempts) * 1000; // Exponential backoff: 1s, 2s, 4s, 8s, 16s
      reconnectAttempts++;
      
      console.log(`🔄 Scheduling reconnection attempt ${reconnectAttempts} in ${delay}ms`);
      reconnectTimeout = setTimeout(createSimulatorWebSocket, delay);
    };

    // Start the initial connection
    createSimulatorWebSocket();
    
    // Pipelines WebSocket (for analytics and fraud detection)
    console.log('🔌 Attempting to connect to pipelines WebSocket:', pipelinesWsUrl);
    pipelinesWs = new WebSocket(pipelinesWsUrl);
    pipelinesWs.onopen = () => {
      console.log('✅ Connected to pipelines WebSocket successfully');
    };

    pipelinesWs.onmessage = (event) => {
      const message = JSON.parse(event.data);
      console.log('📊 Pipelines message received:', message);
      
      if (message.type === 'sales_update') {
        setSalesData(message.data);
      } else if (message.type === 'fraud_alert') {
        setFraudAlerts(prev => [message.data, ...prev.slice(0, 19)]); // Keep last 20 alerts
      }
    };

    pipelinesWs.onerror = (error) => {
      console.error('❌ Pipelines WebSocket error:', error);
      console.error('❌ Connection URL was:', pipelinesWsUrl);
    };

    pipelinesWs.onclose = (event) => {
      console.log('❌ Pipelines WebSocket closed:', {
        code: event.code,
        reason: event.reason,
        wasClean: event.wasClean,
        url: pipelinesWsUrl
      });
    };

    // Cleanup function
    return () => {
      // Clear any pending reconnection timeout
      if (reconnectTimeout) {
        clearTimeout(reconnectTimeout);
      }
      
      // Close WebSocket connections
      if (simulatorWs && simulatorWs.readyState !== WebSocket.CLOSED) {
        simulatorWs.close();
      }
      if (pipelinesWs && pipelinesWs.readyState !== WebSocket.CLOSED) {
        pipelinesWs.close();
      }
    };
  }, []);

  // Fetch initial data
  useEffect(() => {
    const fetchData = async () => {
      try {
        const pipelinesUrl = process.env.REACT_APP_PIPELINES_URL || 'http://localhost:8082';
        
        // Get current sales data from pipelines
        const salesResponse = await fetch(`${pipelinesUrl}/api/sales`);
        if (salesResponse.ok) {
          const sales = await salesResponse.json();
          setSalesData(sales);
        }

        // Get current fraud alerts
        const fraudResponse = await fetch(`${pipelinesUrl}/api/fraud`);
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
            onClearEvents={clearEvents}
          />
        </div>

        <div className="metrics-section">
          <SalesMetrics data={salesData} realTimeSales={realTimeSales} />
        </div>

        <div className="fraud-section">
          <FraudDetection alerts={fraudAlerts} />
        </div>

        <div className="events-section">
          <EventStream events={recentEvents} eventStats={eventStats} />
        </div>
      </div>
    </div>
  );
}

export default App;