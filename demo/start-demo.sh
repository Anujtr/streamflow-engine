#!/bin/bash

# StreamFlow Engine Demo Startup Script

set -e

echo "🌊 Starting StreamFlow Engine Demo..."
echo "=================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose > /dev/null 2>&1; then
    echo "❌ Error: docker-compose is not installed or not in PATH."
    exit 1
fi

# Create necessary directories
echo "📁 Creating data directories..."
mkdir -p data/streamflow
mkdir -p data/etcd
mkdir -p data/prometheus
mkdir -p data/grafana

# Build images if they don't exist
echo "🏗️  Building Docker images..."
docker-compose build --parallel

# Start services in order
echo "🚀 Starting core services..."
docker-compose up -d etcd
echo "⏳ Waiting for etcd to be ready..."
sleep 10

docker-compose up -d streamflow
echo "⏳ Waiting for StreamFlow to be ready..."
sleep 15

echo "📊 Starting monitoring services..."
docker-compose up -d prometheus grafana

echo "🎭 Starting demo services..."
docker-compose up -d simulator pipelines dashboard

# Wait for all services to be healthy
echo "⏳ Waiting for all services to be ready..."
sleep 30

# Check service health
echo "🏥 Checking service health..."
services=("streamflow:8081" "simulator:8000" "pipelines:8081")
all_healthy=true

for service in "${services[@]}"; do
    IFS=':' read -r name port <<< "$service"
    if curl -f -s "http://localhost:$port/health" > /dev/null; then
        echo "✅ $name is healthy"
    else
        echo "❌ $name is not responding"
        all_healthy=false
    fi
done

if [ "$all_healthy" = true ]; then
    echo ""
    echo "🎉 Demo is ready!"
    echo "=================="
    echo ""
    echo "📱 React Dashboard:    http://localhost:3000"
    echo "🎛️  Event Simulator:   http://localhost:8000"
    echo "⚙️  StreamFlow Engine: http://localhost:8080 (gRPC), http://localhost:8081 (HTTP/metrics)"
    echo "📊 Grafana:           http://localhost:3001 (admin/streamflow123)"
    echo "📈 Prometheus:        http://localhost:9091"
    echo ""
    echo "🎮 Demo Commands:"
    echo "curl -X POST http://localhost:8000/start-pattern/normal      # Normal traffic"
    echo "curl -X POST http://localhost:8000/start-pattern/flash_sale  # Flash sale"
    echo "curl -X POST http://localhost:8000/start-pattern/fraud_attack # Fraud simulation"
    echo "curl -X POST http://localhost:8000/stop-pattern              # Stop traffic"
    echo ""
    echo "📋 View logs: docker-compose logs -f"
    echo "🛑 Stop demo: docker-compose down"
    echo ""
else
    echo ""
    echo "⚠️  Some services are not healthy. Check logs with:"
    echo "docker-compose logs"
    echo ""
    echo "You can still try accessing the services above."
fi

# Optionally start a demo pattern
read -p "🎯 Start normal traffic pattern now? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🚀 Starting normal traffic pattern..."
    curl -X POST http://localhost:8000/start-pattern/normal
    echo "✅ Normal traffic pattern started!"
    echo "🎯 Visit http://localhost:3000 to see live data"
fi

echo ""
echo "🎊 Happy streaming with StreamFlow Engine!"