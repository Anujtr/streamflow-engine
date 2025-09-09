#!/usr/bin/env python3
"""
Comprehensive integration test suite for StreamFlow Engine Demo
"""
import asyncio
import json
import time
import requests
import websocket
from typing import Dict, Any, List
import logging
import sys
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add simulator directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../simulator'))

class DemoIntegrationTest:
    """Integration test suite for the complete demo stack"""
    
    def __init__(self):
        self.simulator_url = "http://localhost:8000"
        self.pipelines_url = "http://localhost:8081"
        self.streamflow_url = "http://localhost:8080"
        self.dashboard_url = "http://localhost:3000"
        
        self.test_results = {}
        self.failed_tests = []
    
    def test_service_health(self) -> bool:
        """Test that all services are running and healthy"""
        logger.info("Testing service health...")
        
        services = {
            "Simulator": f"{self.simulator_url}/health",
            "Pipelines": f"{self.pipelines_url}/health", 
            "StreamFlow": f"{self.streamflow_url}/health",
            "Dashboard": f"{self.dashboard_url}/"
        }
        
        all_healthy = True
        for service_name, url in services.items():
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    logger.info(f"✅ {service_name} is healthy")
                else:
                    logger.error(f"❌ {service_name} returned status {response.status_code}")
                    all_healthy = False
            except Exception as e:
                logger.error(f"❌ {service_name} is not responding: {e}")
                all_healthy = False
        
        self.test_results["service_health"] = all_healthy
        if not all_healthy:
            self.failed_tests.append("service_health")
        
        return all_healthy
    
    def test_rest_apis(self) -> bool:
        """Test REST API endpoints"""
        logger.info("Testing REST API endpoints...")
        
        endpoints = {
            "Simulator Stats": f"{self.simulator_url}/stats",
            "Simulator Patterns": f"{self.simulator_url}/patterns",
            "Pipeline Sales": f"{self.pipelines_url}/api/sales",
            "Pipeline Fraud": f"{self.pipelines_url}/api/fraud"
        }
        
        all_working = True
        for endpoint_name, url in endpoints.items():
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    logger.info(f"✅ {endpoint_name} API working")
                    # Validate JSON response
                    data = response.json()
                    if isinstance(data, dict):
                        logger.debug(f"Response keys: {list(data.keys())}")
                    else:
                        logger.debug(f"Response type: {type(data)}")
                else:
                    logger.error(f"❌ {endpoint_name} returned status {response.status_code}")
                    all_working = False
            except Exception as e:
                logger.error(f"❌ {endpoint_name} API error: {e}")
                all_working = False
        
        self.test_results["rest_apis"] = all_working
        if not all_working:
            self.failed_tests.append("rest_apis")
        
        return all_working
    
    def test_traffic_patterns(self) -> bool:
        """Test traffic pattern generation"""
        logger.info("Testing traffic pattern generation...")
        
        patterns = ["normal", "flash_sale", "fraud_attack", "peak_hours"]
        all_working = True
        
        for pattern in patterns:
            try:
                # Start pattern
                logger.info(f"Testing pattern: {pattern}")
                start_response = requests.post(
                    f"{self.simulator_url}/start-pattern/{pattern}", 
                    timeout=10
                )
                
                if start_response.status_code != 200:
                    logger.error(f"❌ Failed to start pattern {pattern}: {start_response.status_code}")
                    all_working = False
                    continue
                
                # Wait for events to generate
                time.sleep(5)
                
                # Check stats
                stats_response = requests.get(f"{self.simulator_url}/stats", timeout=10)
                if stats_response.status_code == 200:
                    stats = stats_response.json()
                    event_count = stats.get("generator_stats", {}).get("total_events", 0)
                    if event_count > 0:
                        logger.info(f"✅ Pattern {pattern} generated {event_count} events")
                    else:
                        logger.warning(f"⚠️ Pattern {pattern} generated no events")
                else:
                    logger.error(f"❌ Could not get stats for pattern {pattern}")
                    all_working = False
                
                # Stop pattern
                stop_response = requests.post(f"{self.simulator_url}/stop-pattern", timeout=10)
                if stop_response.status_code != 200:
                    logger.error(f"❌ Failed to stop pattern {pattern}")
                    all_working = False
                
                # Brief pause between patterns
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Error testing pattern {pattern}: {e}")
                all_working = False
        
        self.test_results["traffic_patterns"] = all_working
        if not all_working:
            self.failed_tests.append("traffic_patterns")
        
        return all_working
    
    def test_websocket_connections(self) -> bool:
        """Test WebSocket connections"""
        logger.info("Testing WebSocket connections...")
        
        websockets = {
            "Simulator": "ws://localhost:8000/ws",
            "Pipelines": "ws://localhost:8081/ws"
        }
        
        all_working = True
        for ws_name, ws_url in websockets.items():
            try:
                logger.info(f"Testing WebSocket: {ws_name}")
                
                # Create WebSocket connection
                ws = websocket.create_connection(ws_url, timeout=10)
                
                # Test basic connectivity
                ws.ping()
                
                # Close connection
                ws.close()
                
                logger.info(f"✅ {ws_name} WebSocket working")
                
            except Exception as e:
                logger.error(f"❌ {ws_name} WebSocket error: {e}")
                all_working = False
        
        self.test_results["websocket_connections"] = all_working
        if not all_working:
            self.failed_tests.append("websocket_connections")
        
        return all_working
    
    def test_data_flow(self) -> bool:
        """Test end-to-end data flow from simulator to pipelines"""
        logger.info("Testing end-to-end data flow...")
        
        try:
            # Get initial stats
            initial_response = requests.get(f"{self.pipelines_url}/api/sales", timeout=10)
            if initial_response.status_code == 200:
                initial_stats = initial_response.json()
                initial_revenue = initial_stats.get("total_revenue", 0)
                initial_transactions = initial_stats.get("transaction_count", 0)
            else:
                logger.warning("Could not get initial pipeline stats")
                initial_revenue = 0
                initial_transactions = 0
            
            # Start a pattern that should generate revenue
            logger.info("Starting flash_sale pattern for data flow test")
            start_response = requests.post(
                f"{self.simulator_url}/start-pattern/flash_sale", 
                timeout=10
            )
            
            if start_response.status_code != 200:
                logger.error("Failed to start flash_sale pattern")
                return False
            
            # Wait for data to flow through the system
            logger.info("Waiting for data to flow through system...")
            time.sleep(15)
            
            # Stop pattern
            requests.post(f"{self.simulator_url}/stop-pattern", timeout=10)
            
            # Check if data made it to pipelines
            final_response = requests.get(f"{self.pipelines_url}/api/sales", timeout=10)
            if final_response.status_code == 200:
                final_stats = final_response.json()
                final_revenue = final_stats.get("total_revenue", 0)
                final_transactions = final_stats.get("transaction_count", 0)
                
                revenue_increase = final_revenue - initial_revenue
                transaction_increase = final_transactions - initial_transactions
                
                logger.info(f"Revenue increase: ${revenue_increase:.2f}")
                logger.info(f"Transaction increase: {transaction_increase}")
                
                if revenue_increase > 0 or transaction_increase > 0:
                    logger.info("✅ Data flow working - events reached pipelines")
                    self.test_results["data_flow"] = True
                    return True
                else:
                    logger.warning("⚠️ No data increase detected - may be mock mode")
                    self.test_results["data_flow"] = False
                    self.failed_tests.append("data_flow")
                    return False
            else:
                logger.error("Could not get final pipeline stats")
                self.test_results["data_flow"] = False
                self.failed_tests.append("data_flow")
                return False
                
        except Exception as e:
            logger.error(f"❌ Data flow test error: {e}")
            self.test_results["data_flow"] = False
            self.failed_tests.append("data_flow")
            return False
    
    def test_fraud_detection(self) -> bool:
        """Test fraud detection functionality"""
        logger.info("Testing fraud detection...")
        
        try:
            # Get initial fraud alerts
            initial_response = requests.get(f"{self.pipelines_url}/api/fraud", timeout=10)
            if initial_response.status_code == 200:
                initial_alerts = initial_response.json()
                if isinstance(initial_alerts, list):
                    initial_count = len(initial_alerts)
                else:
                    initial_count = 0
            else:
                initial_count = 0
            
            # Start fraud attack pattern
            logger.info("Starting fraud_attack pattern")
            start_response = requests.post(
                f"{self.simulator_url}/start-pattern/fraud_attack", 
                timeout=10
            )
            
            if start_response.status_code != 200:
                logger.error("Failed to start fraud_attack pattern")
                return False
            
            # Wait for fraud detection to process
            logger.info("Waiting for fraud detection...")
            time.sleep(12)
            
            # Stop pattern
            requests.post(f"{self.simulator_url}/stop-pattern", timeout=10)
            
            # Check for new fraud alerts
            final_response = requests.get(f"{self.pipelines_url}/api/fraud", timeout=10)
            if final_response.status_code == 200:
                final_alerts = final_response.json()
                if isinstance(final_alerts, list):
                    final_count = len(final_alerts)
                    new_alerts = final_count - initial_count
                    
                    logger.info(f"Initial alerts: {initial_count}, Final alerts: {final_count}")
                    
                    if new_alerts > 0:
                        logger.info(f"✅ Fraud detection working - generated {new_alerts} new alerts")
                        self.test_results["fraud_detection"] = True
                        return True
                    else:
                        logger.warning("⚠️ No new fraud alerts generated")
                        self.test_results["fraud_detection"] = False
                        self.failed_tests.append("fraud_detection")
                        return False
                else:
                    logger.error("Unexpected fraud alerts response format")
                    self.test_results["fraud_detection"] = False
                    self.failed_tests.append("fraud_detection")
                    return False
            else:
                logger.error("Could not get fraud alerts")
                self.test_results["fraud_detection"] = False
                self.failed_tests.append("fraud_detection")
                return False
                
        except Exception as e:
            logger.error(f"❌ Fraud detection test error: {e}")
            self.test_results["fraud_detection"] = False
            self.failed_tests.append("fraud_detection")
            return False
    
    def run_all_tests(self) -> Dict[str, bool]:
        """Run all integration tests"""
        logger.info("🚀 Starting comprehensive integration tests...")
        logger.info("=" * 60)
        
        tests = [
            ("Service Health Check", self.test_service_health),
            ("REST API Endpoints", self.test_rest_apis), 
            ("WebSocket Connections", self.test_websocket_connections),
            ("Traffic Pattern Generation", self.test_traffic_patterns),
            ("End-to-End Data Flow", self.test_data_flow),
            ("Fraud Detection", self.test_fraud_detection)
        ]
        
        for test_name, test_func in tests:
            logger.info(f"\n📋 Running test: {test_name}")
            logger.info("-" * 40)
            try:
                result = test_func()
                if result:
                    logger.info(f"✅ {test_name}: PASSED")
                else:
                    logger.error(f"❌ {test_name}: FAILED")
            except Exception as e:
                logger.error(f"💥 {test_name}: ERROR - {e}")
                self.test_results[test_name.lower().replace(" ", "_")] = False
                self.failed_tests.append(test_name.lower().replace(" ", "_"))
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("🏁 TEST SUMMARY")
        logger.info("=" * 60)
        
        total_tests = len(tests)
        passed_tests = sum(1 for result in self.test_results.values() if result)
        failed_tests = total_tests - passed_tests
        
        logger.info(f"Total Tests: {total_tests}")
        logger.info(f"Passed: {passed_tests}")
        logger.info(f"Failed: {failed_tests}")
        
        if failed_tests == 0:
            logger.info("🎉 ALL TESTS PASSED! Demo is fully functional!")
        else:
            logger.error(f"⚠️ {failed_tests} tests failed. Issues detected:")
            for failed_test in self.failed_tests:
                logger.error(f"  - {failed_test}")
        
        return self.test_results

def main():
    """Main test execution"""
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print("Usage: python integration_test.py")
        print("       python integration_test.py --help")
        print("\nRuns comprehensive integration tests for the StreamFlow Engine demo.")
        print("Ensure all services are running before executing tests.")
        return
    
    print("🌊 StreamFlow Engine Demo Integration Tests")
    print("=" * 50)
    print("Ensure all services are running:")
    print("  docker-compose up -d")
    print("  OR run services individually")
    print()
    
    test_runner = DemoIntegrationTest()
    results = test_runner.run_all_tests()
    
    # Exit with error code if tests failed
    if any(not result for result in results.values()):
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()