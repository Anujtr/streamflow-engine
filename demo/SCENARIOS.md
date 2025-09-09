# StreamFlow Engine Demo Scenarios

This document provides detailed scenarios to showcase the StreamFlow Engine's capabilities in real-world situations.

## 🎯 Demo Scenarios Overview

Each scenario demonstrates different aspects of the StreamFlow Engine:

1. **Normal Operations** - Baseline performance and steady-state behavior
2. **Flash Sale** - High-throughput burst handling and scaling
3. **Fraud Detection** - Real-time pattern matching and alerting
4. **Peak Hours** - Sustained high-load performance
5. **System Recovery** - Fault tolerance and recovery capabilities

---

## 📊 Scenario 1: Normal Operations

**Objective**: Demonstrate baseline system performance and standard e-commerce patterns.

### Setup
```bash
# Start the demo
./start-demo.sh

# Begin normal traffic
curl -X POST http://localhost:8000/start-pattern/normal
```

### Expected Behavior
- **Traffic Rate**: 10-50 events/second
- **Event Mix**: 60% page views, 20% searches, 15% cart additions, 5% purchases
- **Processing Latency**: <10ms average
- **Error Rate**: <0.1%

### Key Metrics to Monitor
- Real-time event throughput on dashboard
- Sales analytics showing gradual revenue increase
- User activity patterns across different locations
- System resource utilization staying low

### Duration
Run for 5-10 minutes to establish baseline metrics.

---

## ⚡ Scenario 2: Flash Sale Simulation

**Objective**: Test system behavior under intense traffic bursts and high-concurrency scenarios.

### Setup
```bash
# Ensure normal pattern is stopped
curl -X POST http://localhost:8000/stop-pattern

# Start flash sale pattern
curl -X POST http://localhost:8000/start-pattern/flash_sale
```

### Expected Behavior
- **Traffic Rate**: 100-500 events/second peak
- **Event Mix**: 30% page views, 10% searches, 40% cart additions, 20% purchases
- **Processing Latency**: <50ms P99
- **Revenue Spike**: Significant increase in sales metrics

### Key Observations
1. **Throughput Scaling**: Monitor how the system handles 10x traffic increase
2. **Queue Management**: Watch for any backlog in message processing
3. **Resource Utilization**: CPU and memory usage increase
4. **Dashboard Responsiveness**: Real-time updates continue smoothly
5. **Sales Analytics**: Revenue timeline shows sharp increase

### Testing Points
- [ ] System maintains low latency under load
- [ ] No message loss occurs during peak traffic
- [ ] Dashboard updates remain real-time
- [ ] All processing pipelines continue operating
- [ ] Fraud detection still functions correctly

### Duration
Run for 3-5 minutes to see peak performance, then stop to observe cool-down.

---

## 🚨 Scenario 3: Fraud Detection

**Objective**: Demonstrate real-time fraud detection and alerting capabilities.

### Setup
```bash
# Stop any running patterns
curl -X POST http://localhost:8000/stop-pattern

# Start fraud attack simulation
curl -X POST http://localhost:8000/start-pattern/fraud_attack
```

### Expected Behavior
- **Traffic Rate**: 50-100 events/second
- **Fraud Indicators**: High-value purchases, multiple locations, suspicious patterns
- **Alert Generation**: Real-time fraud alerts appear in dashboard
- **Risk Scoring**: Events scored and flagged based on risk level

### Fraud Patterns to Observe
1. **High-Value Transactions**: Purchases >$1000
2. **Geographic Anomalies**: Same user from multiple locations
3. **Velocity Attacks**: Multiple rapid purchases
4. **Suspicious Metadata**: Events marked as suspicious
5. **Quantity Abuse**: Large quantity purchases

### Key Metrics
- **Fraud Alerts**: Should see 3-10 alerts generated
- **Risk Scores**: Range from 0.3 to 0.9
- **Alert Severity**: Mix of medium and high severity alerts
- **Session Windows**: 5-minute fraud detection windows
- **Pattern Detection**: Real-time suspicious behavior identification

### Testing Checklist
- [ ] Fraud alerts appear in dashboard within seconds
- [ ] Alert details include related events and risk scores
- [ ] Different severity levels are properly classified
- [ ] Session windowing correctly groups related events
- [ ] WebSocket updates deliver alerts in real-time

### Duration
Run for 5 minutes to generate sufficient fraud patterns and alerts.

---

## 🔥 Scenario 4: Peak Hours

**Objective**: Test sustained high-load performance over extended periods.

### Setup
```bash
# Stop current pattern
curl -X POST http://localhost:8000/stop-pattern

# Start peak hours simulation
curl -X POST http://localhost:8000/start-pattern/peak_hours
```

### Expected Behavior
- **Traffic Rate**: 50-200 events/second sustained
- **Event Distribution**: Realistic user session patterns
- **System Stability**: Consistent performance over time
- **Resource Management**: Efficient resource utilization

### Long-term Metrics
1. **Memory Usage**: Should remain stable, no memory leaks
2. **Processing Latency**: Consistent latency distribution
3. **Throughput**: Sustained event processing rates
4. **Error Rates**: Minimal errors over extended period
5. **Data Growth**: Analytics data accumulates properly

### Monitoring Focus
- **Resource Trends**: CPU, memory, disk usage over time
- **Performance Consistency**: Latency percentiles remain stable
- **Data Accuracy**: Sales metrics accurately reflect event volume
- **System Health**: All services remain responsive

### Duration
Run for 15-30 minutes to observe long-term behavior patterns.

---

## 🔄 Scenario 5: System Recovery

**Objective**: Demonstrate fault tolerance and recovery capabilities.

### Setup
```bash
# Start with normal pattern
curl -X POST http://localhost:8000/start-pattern/normal

# Simulate service failures
docker-compose stop pipelines  # Stop processing pipelines
```

### Recovery Test Sequence
1. **Service Interruption**: Stop processing pipelines
2. **Event Accumulation**: Events continue flowing to StreamFlow
3. **Service Recovery**: Restart the stopped service
4. **Catch-up Processing**: Observe backlog processing
5. **Normal Operation**: Return to steady state

### Expected Recovery Behavior
- Events continue to be received and stored
- Dashboard may show temporary data lag
- Upon restart, processing resumes automatically
- Backlogged events are processed rapidly
- System returns to normal operation

### Recovery Commands
```bash
# Restart stopped service
docker-compose start pipelines

# Monitor recovery
docker-compose logs -f pipelines
```

### Testing Points
- [ ] No event loss during service downtime
- [ ] Automatic reconnection upon service restart
- [ ] Backlog processing without manual intervention
- [ ] Dashboard updates resume automatically
- [ ] Processing latency returns to normal levels

---

## 📈 Performance Benchmarking

### Baseline Performance Test
```bash
# Test sequence
curl -X POST http://localhost:8000/start-pattern/normal
sleep 300  # 5 minutes
curl -X POST http://localhost:8000/start-pattern/flash_sale  
sleep 180  # 3 minutes
curl -X POST http://localhost:8000/start-pattern/peak_hours
sleep 600  # 10 minutes
curl -X POST http://localhost:8000/stop-pattern
```

### Key Performance Indicators (KPIs)
1. **Throughput**: Messages processed per second
2. **Latency**: P50, P95, P99 processing latency
3. **Error Rate**: Percentage of failed message processing
4. **Resource Efficiency**: CPU/memory usage per message
5. **Recovery Time**: Time to resume normal operation after failure

### Expected Performance Targets
- **Normal Traffic**: 50+ events/second, <10ms latency
- **Flash Sale**: 500+ events/second peak, <50ms P99 latency  
- **Peak Hours**: 200+ events/second sustained, stable performance
- **Recovery**: <30 seconds to resume normal operation

---

## 🎭 Demo Presentation Flow

### Recommended Demo Sequence (20 minutes)

1. **Introduction** (2 minutes)
   - Show architecture diagram
   - Explain components and data flow

2. **Normal Operations** (3 minutes)
   - Start normal pattern
   - Tour dashboard components
   - Explain real-time analytics

3. **Flash Sale Demo** (4 minutes)
   - Switch to flash sale pattern
   - Show throughput increase
   - Highlight system scaling

4. **Fraud Detection** (5 minutes)
   - Start fraud attack pattern
   - Show real-time alerts
   - Explain pattern detection

5. **Monitoring & Metrics** (3 minutes)
   - Tour Grafana dashboards
   - Show Prometheus metrics
   - Explain operational monitoring

6. **System Resilience** (2 minutes)
   - Briefly demonstrate recovery
   - Explain fault tolerance

7. **Q&A and Wrap-up** (1 minute)

### Demo Tips
- Have multiple browser tabs open for different services
- Use full-screen mode for better visibility
- Prepare for common questions about scalability
- Have backup plan if services are slow to start
- Monitor resource usage to prevent overload

---

## 🐛 Troubleshooting Common Issues

### Services Not Starting
```bash
# Check Docker resources
docker system df
docker system prune  # If needed

# Check port conflicts
netstat -tulpn | grep :3000
netstat -tulpn | grep :8000
netstat -tulpn | grep :8080
```

### High Resource Usage
```bash
# Monitor resource usage
docker stats

# Adjust memory limits in docker-compose.yml
# Reduce traffic pattern intensity
```

### WebSocket Connection Issues
```bash
# Check service health
curl http://localhost:8080/health
curl http://localhost:8000/health

# Restart services
docker-compose restart pipelines simulator
```

### Data Not Appearing in Dashboard
```bash
# Check service logs
docker-compose logs simulator
docker-compose logs pipelines

# Verify WebSocket connections in browser console
# Check network tab for failed requests
```

---

## 📊 Success Criteria

### Functional Requirements
- [ ] All services start successfully
- [ ] Dashboard displays real-time data
- [ ] Traffic patterns generate expected event volumes
- [ ] Fraud detection creates alerts
- [ ] System handles traffic spikes gracefully
- [ ] Recovery from service failures works

### Performance Requirements
- [ ] Normal traffic: <10ms average latency
- [ ] Flash sale: handles 500+ events/second
- [ ] Peak hours: stable for 30+ minutes
- [ ] Recovery: <30 seconds back to normal
- [ ] Error rate: <1% under all conditions

### User Experience Requirements
- [ ] Dashboard is responsive and intuitive
- [ ] Real-time updates work smoothly
- [ ] Fraud alerts are actionable and clear
- [ ] Monitoring dashboards provide clear insights
- [ ] Demo scenarios tell a compelling story

---

This completes the comprehensive scenario guide for the StreamFlow Engine demo. Each scenario is designed to highlight different strengths of the system and provide a complete picture of its capabilities.