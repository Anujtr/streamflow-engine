#!/bin/bash

# StreamFlow Engine Demo Validation Script
# Comprehensive validation of demo functionality

set -e

echo "🌊 StreamFlow Engine Demo Validation"
echo "====================================="

# Configuration
SIMULATOR_URL="http://localhost:8000"
PIPELINES_URL="http://localhost:8081"
STREAMFLOW_URL="http://localhost:8080"
DASHBOARD_URL="http://localhost:3000"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test results
TESTS_PASSED=0
TESTS_FAILED=0
FAILED_TESTS=()

# Helper functions
check_service() {
    local name="$1"
    local url="$2"
    
    echo -n "Checking $name... "
    
    if curl -f -s "$url" > /dev/null; then
        echo -e "${GREEN}✅ OK${NC}"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}❌ FAILED${NC}"
        FAILED_TESTS+=("$name service")
        ((TESTS_FAILED++))
        return 1
    fi
}

check_api_endpoint() {
    local name="$1"
    local url="$2"
    
    echo -n "Testing $name API... "
    
    response=$(curl -f -s "$url" 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$response" ]; then
        echo -e "${GREEN}✅ OK${NC}"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}❌ FAILED${NC}"
        FAILED_TESTS+=("$name API")
        ((TESTS_FAILED++))
        return 1
    fi
}

test_traffic_pattern() {
    local pattern="$1"
    local duration="$2"
    
    echo -n "Testing traffic pattern '$pattern'... "
    
    # Start pattern
    start_response=$(curl -f -s -X POST "$SIMULATOR_URL/start-pattern/$pattern" 2>/dev/null)
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ FAILED to start${NC}"
        FAILED_TESTS+=("Traffic pattern $pattern")
        ((TESTS_FAILED++))
        return 1
    fi
    
    # Wait for pattern to generate events
    sleep "$duration"
    
    # Check stats
    stats_response=$(curl -f -s "$SIMULATOR_URL/stats" 2>/dev/null)
    if [ $? -eq 0 ]; then
        event_count=$(echo "$stats_response" | python3 -c "import json, sys; data=json.load(sys.stdin); print(data.get('generator_stats', {}).get('total_events', 0))" 2>/dev/null || echo "0")
        
        if [ "$event_count" -gt 0 ]; then
            echo -e "${GREEN}✅ OK${NC} ($event_count events)"
            ((TESTS_PASSED++))
        else
            echo -e "${YELLOW}⚠️  WARNING${NC} (no events generated)"
            ((TESTS_FAILED++))
            FAILED_TESTS+=("Traffic pattern $pattern - no events")
        fi
    else
        echo -e "${RED}❌ FAILED${NC} (stats check failed)"
        FAILED_TESTS+=("Traffic pattern $pattern")
        ((TESTS_FAILED++))
    fi
    
    # Stop pattern
    curl -f -s -X POST "$SIMULATOR_URL/stop-pattern" >/dev/null 2>&1
    
    # Brief pause
    sleep 2
}

test_websocket_connection() {
    local name="$1"
    local url="$2"
    
    echo -n "Testing WebSocket $name... "
    
    # Use Python to test WebSocket connection
    python3 -c "
import websocket
import sys

try:
    ws = websocket.create_connection('$url', timeout=10)
    ws.ping()
    ws.close()
    sys.exit(0)
except Exception as e:
    sys.exit(1)
" 2>/dev/null

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ OK${NC}"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}❌ FAILED${NC}"
        FAILED_TESTS+=("WebSocket $name")
        ((TESTS_FAILED++))
        return 1
    fi
}

# Check if required tools are available
check_dependencies() {
    echo "Checking dependencies..."
    
    local deps=("curl" "python3")
    local missing=()
    
    for dep in "${deps[@]}"; do
        if ! command -v "$dep" &> /dev/null; then
            missing+=("$dep")
        fi
    done
    
    if [ ${#missing[@]} -gt 0 ]; then
        echo -e "${RED}❌ Missing dependencies: ${missing[*]}${NC}"
        echo "Please install missing dependencies and retry."
        exit 1
    fi
    
    # Check if websocket-client is available
    python3 -c "import websocket" 2>/dev/null || {
        echo -e "${YELLOW}⚠️  Installing websocket-client...${NC}"
        pip3 install websocket-client >/dev/null 2>&1 || {
            echo -e "${YELLOW}⚠️  Could not install websocket-client, WebSocket tests will be skipped${NC}"
        }
    }
    
    echo -e "${GREEN}✅ Dependencies check passed${NC}"
}

# Main validation sequence
main() {
    echo
    echo "📋 Starting comprehensive demo validation..."
    echo
    
    # Check dependencies
    check_dependencies
    echo
    
    # Phase 1: Service Health Checks
    echo "🔍 Phase 1: Service Health Checks"
    echo "--------------------------------"
    check_service "Simulator" "$SIMULATOR_URL/health"
    check_service "Pipelines" "$PIPELINES_URL/health"
    check_service "StreamFlow" "$STREAMFLOW_URL/health"
    check_service "Dashboard" "$DASHBOARD_URL/"
    echo
    
    # Phase 2: API Endpoint Tests
    echo "🔌 Phase 2: API Endpoint Tests"
    echo "------------------------------"
    check_api_endpoint "Simulator Stats" "$SIMULATOR_URL/stats"
    check_api_endpoint "Simulator Patterns" "$SIMULATOR_URL/patterns"
    check_api_endpoint "Pipeline Sales" "$PIPELINES_URL/api/sales"
    check_api_endpoint "Pipeline Fraud" "$PIPELINES_URL/api/fraud"
    echo
    
    # Phase 3: WebSocket Connection Tests
    echo "🔗 Phase 3: WebSocket Connection Tests"
    echo "-------------------------------------"
    test_websocket_connection "Simulator" "ws://localhost:8000/ws"
    test_websocket_connection "Pipelines" "ws://localhost:8081/ws"
    echo
    
    # Phase 4: Traffic Pattern Tests
    echo "🎭 Phase 4: Traffic Pattern Tests"
    echo "--------------------------------"
    test_traffic_pattern "normal" 5
    test_traffic_pattern "flash_sale" 5
    test_traffic_pattern "fraud_attack" 5
    test_traffic_pattern "peak_hours" 5
    echo
    
    # Phase 5: Data Flow Validation
    echo "🔄 Phase 5: Data Flow Validation"
    echo "-------------------------------"
    echo -n "Testing end-to-end data flow... "
    
    # Get initial metrics
    initial_metrics=$(curl -f -s "$PIPELINES_URL/api/sales" 2>/dev/null)
    
    # Start flash sale pattern
    curl -f -s -X POST "$SIMULATOR_URL/start-pattern/flash_sale" >/dev/null 2>&1
    
    # Wait for data to flow
    sleep 15
    
    # Stop pattern
    curl -f -s -X POST "$SIMULATOR_URL/stop-pattern" >/dev/null 2>&1
    
    # Check final metrics
    final_metrics=$(curl -f -s "$PIPELINES_URL/api/sales" 2>/dev/null)
    
    if [ -n "$initial_metrics" ] && [ -n "$final_metrics" ]; then
        echo -e "${GREEN}✅ OK${NC} (data flow validated)"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}❌ FAILED${NC} (could not validate data flow)"
        FAILED_TESTS+=("Data flow validation")
        ((TESTS_FAILED++))
    fi
    echo
    
    # Summary
    echo "📊 VALIDATION SUMMARY"
    echo "===================="
    echo -e "Total Tests:   $((TESTS_PASSED + TESTS_FAILED))"
    echo -e "Passed:        ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Failed:        ${RED}$TESTS_FAILED${NC}"
    echo
    
    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}🎉 ALL TESTS PASSED! Demo is fully functional!${NC}"
        echo
        echo "🚀 Demo is ready for presentation!"
        echo "================================="
        echo "Access the demo at:"
        echo "  📱 Dashboard:    http://localhost:3000"
        echo "  🎛️  Simulator:   http://localhost:8000"
        echo "  📊 Grafana:     http://localhost:3001 (admin/streamflow123)"
        echo
        exit 0
    else
        echo -e "${RED}⚠️  $TESTS_FAILED TESTS FAILED${NC}"
        echo
        echo "Failed tests:"
        for failed_test in "${FAILED_TESTS[@]}"; do
            echo -e "  ${RED}❌ $failed_test${NC}"
        done
        echo
        echo "Please fix the issues above before using the demo."
        exit 1
    fi
}

# Show usage if help requested
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "StreamFlow Engine Demo Validation Script"
    echo
    echo "Usage: $0 [options]"
    echo
    echo "Options:"
    echo "  --help, -h    Show this help message"
    echo
    echo "This script performs comprehensive validation of the StreamFlow Engine demo:"
    echo "  1. Service health checks"
    echo "  2. API endpoint tests"
    echo "  3. WebSocket connection tests"
    echo "  4. Traffic pattern functionality"
    echo "  5. End-to-end data flow validation"
    echo
    echo "Prerequisites:"
    echo "  - All demo services must be running"
    echo "  - curl and python3 must be installed"
    echo
    echo "Example:"
    echo "  # Start services"
    echo "  docker-compose up -d"
    echo
    echo "  # Run validation"
    echo "  $0"
    exit 0
fi

# Run main validation
main