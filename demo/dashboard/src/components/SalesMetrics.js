import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, 
         BarChart, Bar, PieChart, Pie, Cell } from 'recharts';

const SalesMetrics = ({ data }) => {
  if (!data) {
    return (
      <div>
        <div className="component-header">
          <span className="icon">💰</span>
          <h2>Sales Analytics</h2>
        </div>
        <div className="loading">
          <div className="spinner"></div>
          Loading sales data...
        </div>
      </div>
    );
  }

  const {
    total_revenue = 0,
    transaction_count = 0,
    average_order_value = 0,
    top_products = [],
    sales_by_category = [],
    sales_by_location = [],
    revenue_timeline = []
  } = data;

  // Format numbers for display
  const formatCurrency = (value) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  };

  const formatNumber = (value) => {
    return new Intl.NumberFormat('en-US').format(value);
  };

  // Prepare timeline data
  const timelineData = revenue_timeline.map(point => ({
    time: new Date(point.timestamp).toLocaleTimeString('en-US', { 
      hour: '2-digit', 
      minute: '2-digit' 
    }),
    revenue: point.revenue,
    orders: point.orders
  }));

  // Prepare category data for pie chart
  const categoryColors = ['#8884d8', '#82ca9d', '#ffc658', '#ff7300', '#00ff00'];
  const categoryData = sales_by_category.slice(0, 5).map((cat, index) => ({
    name: cat.category,
    value: cat.revenue,
    color: categoryColors[index % categoryColors.length]
  }));

  // Prepare top products data
  const productData = top_products.slice(0, 5).map(product => ({
    name: product.product_name.substring(0, 15) + '...',
    revenue: product.revenue,
    units: product.units
  }));

  return (
    <div>
      <div className="component-header">
        <span className="icon">💰</span>
        <h2>Sales Analytics</h2>
        <div className="last-updated">
          Last updated: {new Date(data.last_updated).toLocaleTimeString()}
        </div>
      </div>

      {/* Key Metrics */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-value">{formatCurrency(total_revenue)}</div>
          <div className="stat-label">Total Revenue</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{formatNumber(transaction_count)}</div>
          <div className="stat-label">Transactions</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{formatCurrency(average_order_value)}</div>
          <div className="stat-label">Avg Order Value</div>
        </div>
      </div>

      {/* Charts Section */}
      <div className="charts-container">
        
        {/* Revenue Timeline */}
        {timelineData.length > 0 && (
          <div className="chart-section">
            <h3>Revenue Timeline (Last Hour)</h3>
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={timelineData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis 
                  dataKey="time" 
                  tick={{ fontSize: 12 }}
                  interval="preserveStartEnd"
                />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip 
                  formatter={(value, name) => [
                    name === 'revenue' ? formatCurrency(value) : formatNumber(value),
                    name === 'revenue' ? 'Revenue' : 'Orders'
                  ]}
                />
                <Line 
                  type="monotone" 
                  dataKey="revenue" 
                  stroke="#8884d8" 
                  strokeWidth={2}
                  dot={{ r: 3 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Charts Grid */}
        <div className="charts-grid">
          
          {/* Top Products */}
          {productData.length > 0 && (
            <div className="chart-section">
              <h3>Top Products by Revenue</h3>
              <ResponsiveContainer width="100%" height={250}>
                <BarChart data={productData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    dataKey="name" 
                    tick={{ fontSize: 11 }}
                    angle={-45}
                    textAnchor="end"
                    height={70}
                  />
                  <YAxis tick={{ fontSize: 11 }} />
                  <Tooltip 
                    formatter={(value, name) => [
                      name === 'revenue' ? formatCurrency(value) : formatNumber(value),
                      name === 'revenue' ? 'Revenue' : 'Units Sold'
                    ]}
                  />
                  <Bar dataKey="revenue" fill="#82ca9d" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Sales by Category */}
          {categoryData.length > 0 && (
            <div className="chart-section">
              <h3>Sales by Category</h3>
              <ResponsiveContainer width="100%" height={250}>
                <PieChart>
                  <Pie
                    data={categoryData}
                    cx="50%"
                    cy="50%"
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="value"
                    label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                  >
                    {categoryData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value) => [formatCurrency(value), 'Revenue']} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          )}

        </div>

        {/* Location Data */}
        {sales_by_location.length > 0 && (
          <div className="location-section">
            <h3>Sales by Location</h3>
            <div className="location-grid">
              {sales_by_location.slice(0, 6).map((location, index) => (
                <div key={index} className="location-card">
                  <div className="location-name">{location.location}</div>
                  <div className="location-stats">
                    <span className="location-revenue">{formatCurrency(location.revenue)}</span>
                    <span className="location-orders">{formatNumber(location.orders)} orders</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

      </div>

      <style jsx>{`
        .last-updated {
          margin-left: auto;
          font-size: 0.75rem;
          color: #6c757d;
          background: #f8f9fa;
          padding: 0.25rem 0.5rem;
          border-radius: 4px;
        }

        .charts-container {
          margin-top: 1rem;
        }

        .chart-section {
          margin-bottom: 2rem;
        }

        .chart-section h3 {
          margin: 0 0 1rem 0;
          font-size: 1.1rem;
          color: #495057;
        }

        .charts-grid {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 2rem;
          margin: 2rem 0;
        }

        .location-section h3 {
          margin: 2rem 0 1rem 0;
          font-size: 1.1rem;
          color: #495057;
        }

        .location-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: 1rem;
        }

        .location-card {
          background: #f8f9fa;
          border-radius: 8px;
          padding: 1rem;
          text-align: center;
          border: 1px solid #e9ecef;
        }

        .location-name {
          font-weight: 600;
          color: #495057;
          margin-bottom: 0.5rem;
        }

        .location-stats {
          display: flex;
          flex-direction: column;
          gap: 0.25rem;
        }

        .location-revenue {
          font-size: 1.1rem;
          font-weight: 600;
          color: #28a745;
        }

        .location-orders {
          font-size: 0.875rem;
          color: #6c757d;
        }

        @media (max-width: 768px) {
          .charts-grid {
            grid-template-columns: 1fr;
          }
          
          .location-grid {
            grid-template-columns: 1fr;
          }
        }
      `}</style>
    </div>
  );
};

export default SalesMetrics;