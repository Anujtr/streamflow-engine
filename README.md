# StreamFlow Engine

> Real-time stream processing for high-performance applications

[![Go](https://img.shields.io/badge/Go-1.24%2B-00ADD8?style=flat&logo=go)](https://golang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=flat&logo=docker)](https://www.docker.com/)

StreamFlow Engine is a distributed stream processing system built for developers who need to process high-volume real-time data with minimal latency. Whether you're building analytics pipelines, fraud detection systems, or real-time dashboards, StreamFlow provides the performance and reliability you need.

## ✨ Why StreamFlow?

- **High Performance** - Process 50K+ messages per second with sub-millisecond latency
- **Simple APIs** - Intuitive producer/consumer clients and fluent stream processing
- **Production Ready** - Built-in persistence, monitoring, and fault tolerance
- **Easy Deployment** - Docker support with monitoring stack included
- **Real-time Processing** - Advanced windowing, joins, and pattern detection

## 🎯 Try the Demo

See StreamFlow in action with a complete e-commerce analytics system:

```bash
git clone https://github.com/Anujtr/streamflow-engine
cd streamflow-engine/demo
./start-demo.sh
```

Open http://localhost:3000 to see real-time sales analytics, fraud detection, and event processing in action.

## 🛠️ Quick Start

### Using Docker (Recommended)
```bash
git clone https://github.com/Anujtr/streamflow-engine
cd streamflow-engine
docker-compose up -d
```

### From Source
```bash
go build -o streamflow ./cmd/streamflow/
./streamflow
```

### Basic Usage

**Send and receive messages:**
```go
// Send a message
producer, _ := client.NewProducer(client.ProducerConfig{Address: "localhost:8080"})
producer.SendSingle(ctx, "events", "user123", []byte("purchase completed"))

// Receive messages
consumer, _ := client.NewConsumer(client.ConsumerConfig{Address: "localhost:8080"})
messages, _ := consumer.Consume(ctx, "events", 0, 0, 100)
```

**Stream processing:**
```go
// Real-time aggregation
processor.NewStream("sales").
    Window(5 * time.Minute).
    Count().
    ForEach(func(result *stream.AggregateResult) {
        log.Printf("Sales in last 5min: %d", result.Count)
    })
```

## 📊 Performance

StreamFlow delivers production-grade performance:

- **50K+ messages/second** with persistent storage
- **Sub-millisecond latency** for real-time processing  
- **99%+ reliability** in production workloads

*Benchmarks run on standard hardware. See [IMPLEMENTATION.md](IMPLEMENTATION.md) for detailed results.*

## 🏗️ Architecture

StreamFlow is designed for simplicity and scalability:

```
┌─────────────┐              ┌─────────────────┐
│ Your App    │──── gRPC ───▶│   StreamFlow    │
│             │              │     Engine      │
└─────────────┘              └─────────────────┘
                                      │
                                      ▼
                               ┌─────────────┐
                               │  Persistent │
                               │   Storage   │
                               └─────────────┘
```

**Key Components:**
- **Message Broker** - High-throughput message storage and retrieval
- **Stream Processor** - Real-time event processing with windowing and joins
- **Client Libraries** - Simple producer/consumer APIs  
- **Storage** - Durable persistence with automatic recovery
- **Monitoring** - Built-in metrics and health checks

## 📈 Monitoring

Built-in monitoring and observability:

- **Metrics** - Prometheus-compatible metrics for throughput, latency, and errors
- **Dashboards** - Pre-configured Grafana dashboards  
- **Health Checks** - System health monitoring with automatic alerts
- **Performance Profiling** - Built-in profiling for production debugging

Access at `http://localhost:8081/metrics` and `http://localhost:3000` (Grafana)

## 🧪 Testing

```bash
# Run all tests
go test ./...

# Run benchmarks  
go test -bench=. ./...

# Test the demo
cd demo/test && python3 integration_test.py
```

## 🚀 Deployment

**Docker (Recommended):**
```bash
docker-compose up -d
```

**Binary:**
```bash
./streamflow --persistent=true --data-dir=./data
```

For Kubernetes and distributed deployments, see [IMPLEMENTATION.md](IMPLEMENTATION.md).

## 🔄 What's Next

- Kubernetes Helm charts
- Enhanced security features  
- Additional client language support
- More stream processing operations

## 🤝 Contributing

We welcome contributions! Please see our contributing guidelines and:

- Report bugs and request features via GitHub Issues
- Submit pull requests for bug fixes and improvements
- Follow the existing code style and testing patterns
- Add tests for new functionality

See [IMPLEMENTATION.md](IMPLEMENTATION.md) for detailed development information.

## 📚 Documentation

- **[Implementation Guide](IMPLEMENTATION.md)** - Detailed technical documentation
- **[Demo Application](demo/README.md)** - Complete demo setup and usage
- **[Demo Scenarios](demo/SCENARIOS.md)** - Traffic patterns and testing scenarios
- **[API Reference](api/proto/)** - gRPC service definitions

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

**StreamFlow Engine** - Built with ❤️ for high-performance stream processing