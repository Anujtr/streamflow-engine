# StreamFlow Engine Demo Application

This demo showcases the StreamFlow Engine's capabilities through a real-world e-commerce scenario with live analytics, fraud detection, and real-time monitoring.

## 🌟 Features

- **Event Simulation**: Python FastAPI simulator generating realistic e-commerce events
- **Stream Processing**: Go-based pipelines for sales analytics and fraud detection
- **Real-time Dashboard**: React application with live visualizations
- **Fraud Detection**: Pattern-based fraud detection with real-time alerts
- **Monitoring**: Prometheus + Grafana for comprehensive metrics
- **Docker Support**: Complete containerized deployment

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Event Simulator│    │   StreamFlow    │    │   Processing    │
│   (Python)      │───▶│     Engine      │───▶│   Pipelines     │
│   Port: 8000    │    │   Port: 8080    │    │   (Go)          │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       │
                                ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     etcd        │    │   React         │    │   Prometheus    │
│ (Coordination)  │    │   Dashboard     │    │   + Grafana     │
│   Port: 2379    │    │   Port: 3000    │    │   Port: 9091    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Go 1.24+ (for local development)
- Node.js 18+ (for local development)
- Python 3.11+ (for local development)

### Running the Complete Demo

```bash
# Clone the repository
git clone https://github.com/Anujtr/streamflow-engine.git
cd streamflow-engine/demo

# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

### Accessing the Demo

- **React Dashboard**: http://localhost:3000
- **Event Simulator**: http://localhost:8000
- **StreamFlow Engine**: http://localhost:8080
- **Grafana**: http://localhost:3001 (admin/streamflow123)
- **Prometheus**: http://localhost:9091

## 🎮 Demo Scenarios

### 1. Normal Traffic Pattern
```bash
curl -X POST http://localhost:8000/start-pattern/normal
```
- Generates baseline e-commerce traffic
- Mix of page views, searches, cart additions, and purchases
- 10-50 events/second

### 2. Flash Sale Scenario
```bash
curl -X POST http://localhost:8000/start-pattern/flash_sale
```
- High-intensity traffic burst
- Increased purchase and cart activity
- 100-500 events/second peak

### 3. Fraud Attack Simulation
```bash
curl -X POST http://localhost:8000/start-pattern/fraud_attack
```
- Suspicious user behavior patterns
- High-value transactions from multiple locations
- Triggers fraud detection alerts

### 4. Peak Hours
```bash
curl -X POST http://localhost:8000/start-pattern/peak_hours
```
- Sustained high traffic
- Realistic user session patterns
- 50-200 events/second sustained

### Stopping Patterns
```bash
curl -X POST http://localhost:8000/stop-pattern
```

## 📊 Monitoring & Analytics

### Sales Analytics
- **Real-time Revenue**: Track total revenue and transaction counts
- **Product Performance**: Top-selling products and categories
- **Geographic Distribution**: Sales by location
- **Timeline Analysis**: Revenue trends over time

### Fraud Detection
- **Pattern Matching**: Detects suspicious behavior patterns
- **Risk Scoring**: Calculates risk scores for user activities
- **Real-time Alerts**: Immediate notification of fraud attempts
- **Session Analysis**: 5-minute windows for fraud detection

### Performance Metrics
- **Throughput**: Messages processed per second
- **Latency**: Processing latency percentiles
- **Error Rates**: Failed message processing
- **Resource Usage**: CPU, memory, and disk usage

## 🔧 Development

### Running Services Locally

#### StreamFlow Engine
```bash
cd streamflow-engine
go run ./cmd/server
```

#### Event Simulator
```bash
cd demo/simulator
pip install -r requirements.txt
python app.py
```

#### Processing Pipelines
```bash
cd demo/pipelines
go run main.go
```

#### React Dashboard
```bash
cd demo/dashboard
npm install
npm start
```

### API Endpoints

#### Simulator APIs
- `GET /` - Web interface
- `POST /start-pattern/{pattern}` - Start traffic pattern
- `POST /stop-pattern` - Stop current pattern
- `GET /health` - Health check
- `GET /stats` - Current statistics

#### Pipeline APIs
- `GET /api/sales` - Current sales metrics
- `GET /api/fraud` - Current fraud alerts
- `GET /health` - Health check
- `WS /ws` - WebSocket for real-time updates

## 🐳 Docker Configuration

### Building Images

```bash
# Build all images
docker-compose build

# Build specific service
docker-compose build simulator
```

### Environment Variables

#### StreamFlow Engine
- `STREAMFLOW_PORT`: HTTP server port (default: 8080)
- `STREAMFLOW_GRPC_PORT`: gRPC server port (default: 9090)
- `ETCD_ENDPOINTS`: etcd connection string

#### Simulator
- `STREAMFLOW_HOST`: StreamFlow engine host
- `STREAMFLOW_PORT`: StreamFlow gRPC port

#### Pipelines
- `STREAMFLOW_HOST`: StreamFlow engine host
- `PIPELINE_PORT`: WebSocket server port

## 📈 Performance Expectations

### Throughput Targets
- **Normal Traffic**: 10-50 events/second
- **Flash Sale**: 100-500 events/second peak
- **Peak Hours**: 50-200 events/second sustained
- **Processing Latency**: <50ms P99

### Resource Requirements
- **StreamFlow Engine**: 512MB RAM, 1 CPU core
- **Event Simulator**: 256MB RAM, 0.5 CPU core
- **Processing Pipelines**: 256MB RAM, 0.5 CPU core
- **React Dashboard**: 128MB RAM, 0.25 CPU core

## 🛠️ Troubleshooting

### Common Issues

#### Services not starting
```bash
# Check logs
docker-compose logs streamflow

# Verify dependencies
docker-compose ps
```

#### WebSocket connections failing
```bash
# Check port conflicts
netstat -tulpn | grep :8080

# Restart services
docker-compose restart pipelines
```

#### High memory usage
```bash
# Monitor resource usage
docker stats

# Adjust memory limits in docker-compose.yml
```

### Health Checks

All services include health check endpoints:
```bash
curl http://localhost:8000/health  # Simulator
curl http://localhost:8080/health  # StreamFlow
curl http://localhost:8080/health  # Pipelines
```

## 📝 Event Schema

### E-commerce Event Structure
```json
{
  "event_id": "uuid",
  "event_type": "purchase|add_to_cart|page_view|search",
  "user_id": "string",
  "session_id": "uuid",
  "timestamp": "ISO8601",
  "product_id": "string",
  "product_name": "string",
  "product_category": "string",
  "product_price": 99.99,
  "quantity": 1,
  "location": "string",
  "device_type": "desktop|mobile|tablet",
  "metadata": {
    "suspicious": false
  }
}
```

## 🎯 Use Cases

### Real-time Analytics
- Monitor sales performance in real-time
- Track user behavior patterns
- Analyze product popularity trends
- Geographic sales distribution

### Fraud Detection
- Detect suspicious purchase patterns
- Monitor high-risk transactions
- Track user behavior anomalies
- Generate real-time fraud alerts

### Performance Monitoring
- System throughput and latency
- Error rates and failures
- Resource utilization
- Service health status

## 🔮 Future Enhancements

- **Machine Learning**: Advanced fraud detection models
- **A/B Testing**: Traffic splitting capabilities  
- **Multi-tenant**: Support for multiple customers
- **Global Distribution**: Multi-region deployment
- **Advanced Analytics**: Predictive analytics and forecasting

## 📞 Support

For questions or issues:
- Create an issue on GitHub
- Check the main README.md for general information
- Review the implementation-plan.md for technical details