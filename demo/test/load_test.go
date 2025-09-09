package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// LoadTestConfig defines load test parameters
type LoadTestConfig struct {
	SimulatorURL      string
	PipelinesURL      string
	TestDuration      time.Duration
	ConcurrentClients int
	RequestRate       int // requests per second
}

// LoadTestResults stores test results
type LoadTestResults struct {
	TotalRequests    int64
	SuccessRequests  int64 
	FailedRequests   int64
	AverageLatency   time.Duration
	MaxLatency       time.Duration
	MinLatency       time.Duration
	TotalDuration    time.Duration
}

// LoadTester manages load testing
type LoadTester struct {
	config  LoadTestConfig
	results LoadTestResults
	client  *http.Client
	mu      sync.RWMutex
}

// NewLoadTester creates a new load tester
func NewLoadTester(config LoadTestConfig) *LoadTester {
	return &LoadTester{
		config: config,
		client: &http.Client{
			Timeout: time.Second * 10,
		},
		results: LoadTestResults{
			MinLatency: time.Hour, // Initialize to high value
		},
	}
}

// makeRequest performs a single HTTP request and measures latency
func (lt *LoadTester) makeRequest(url string) {
	start := time.Now()
	atomic.AddInt64(&lt.results.TotalRequests, 1)

	resp, err := lt.client.Get(url)
	latency := time.Since(start)

	if err != nil {
		atomic.AddInt64(&lt.results.FailedRequests, 1)
		log.Printf("Request failed: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		atomic.AddInt64(&lt.results.SuccessRequests, 1)
	} else {
		atomic.AddInt64(&lt.results.FailedRequests, 1)
		log.Printf("Request failed with status: %d", resp.StatusCode)
	}

	// Update latency statistics (thread-safe)
	lt.mu.Lock()
	if latency > lt.results.MaxLatency {
		lt.results.MaxLatency = latency
	}
	if latency < lt.results.MinLatency {
		lt.results.MinLatency = latency
	}
	lt.mu.Unlock()
}

// RunHealthCheck tests service availability
func (lt *LoadTester) RunHealthCheck() error {
	log.Println("Running health check...")
	
	endpoints := []string{
		lt.config.SimulatorURL + "/health",
		lt.config.PipelinesURL + "/health",
	}

	for _, endpoint := range endpoints {
		resp, err := lt.client.Get(endpoint)
		if err != nil {
			return fmt.Errorf("health check failed for %s: %v", endpoint, err)
		}
		resp.Body.Close()
		
		if resp.StatusCode != 200 {
			return fmt.Errorf("health check failed for %s: status %d", endpoint, resp.StatusCode)
		}
		
		log.Printf("✅ Health check passed: %s", endpoint)
	}
	
	return nil
}

// RunLoadTest executes the load test
func (lt *LoadTester) RunLoadTest() error {
	log.Printf("Starting load test...")
	log.Printf("Duration: %v", lt.config.TestDuration)
	log.Printf("Concurrent clients: %d", lt.config.ConcurrentClients)
	log.Printf("Target rate: %d req/sec", lt.config.RequestRate)
	
	startTime := time.Now()
	
	// Calculate request interval
	requestInterval := time.Second / time.Duration(lt.config.RequestRate)
	
	// Test endpoints
	endpoints := []string{
		lt.config.SimulatorURL + "/stats",
		lt.config.SimulatorURL + "/patterns", 
		lt.config.PipelinesURL + "/api/sales",
		lt.config.PipelinesURL + "/api/fraud",
	}
	
	// Channel to control test duration
	done := make(chan bool)
	
	// Start timer for test duration
	go func() {
		time.Sleep(lt.config.TestDuration)
		close(done)
	}()
	
	// Worker goroutines
	var wg sync.WaitGroup
	
	for i := 0; i < lt.config.ConcurrentClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			
			ticker := time.NewTicker(requestInterval * time.Duration(lt.config.ConcurrentClients))
			defer ticker.Stop()
			
			endpointIndex := 0
			
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					endpoint := endpoints[endpointIndex%len(endpoints)]
					lt.makeRequest(endpoint)
					endpointIndex++
				}
			}
		}(i)
	}
	
	// Wait for all workers to complete
	wg.Wait()
	
	lt.results.TotalDuration = time.Since(startTime)
	
	// Calculate average latency (approximation)
	if lt.results.SuccessRequests > 0 {
		totalLatency := time.Duration(lt.results.SuccessRequests) * (lt.results.MaxLatency + lt.results.MinLatency) / 2
		lt.results.AverageLatency = totalLatency / time.Duration(lt.results.SuccessRequests)
	}
	
	return nil
}

// RunTrafficPatternTest tests traffic pattern handling under load
func (lt *LoadTester) RunTrafficPatternTest() error {
	log.Println("Testing traffic patterns under load...")
	
	patterns := []string{"normal", "flash_sale", "peak_hours"}
	
	for _, pattern := range patterns {
		log.Printf("Testing pattern: %s", pattern)
		
		// Start pattern
		resp, err := http.Post(lt.config.SimulatorURL+"/start-pattern/"+pattern, "application/json", nil)
		if err != nil {
			return fmt.Errorf("failed to start pattern %s: %v", pattern, err)
		}
		resp.Body.Close()
		
		// Run load test for a short duration
		shortConfig := lt.config
		shortConfig.TestDuration = 30 * time.Second
		shortTester := NewLoadTester(shortConfig)
		
		if err := shortTester.RunLoadTest(); err != nil {
			return fmt.Errorf("load test failed during pattern %s: %v", pattern, err)
		}
		
		log.Printf("Pattern %s completed: %d requests, %d successful", 
			pattern, shortTester.results.TotalRequests, shortTester.results.SuccessRequests)
		
		// Stop pattern
		resp, err = http.Post(lt.config.SimulatorURL+"/stop-pattern", "application/json", nil)
		if err != nil {
			log.Printf("Warning: failed to stop pattern %s: %v", pattern, err)
		} else {
			resp.Body.Close()
		}
		
		// Brief pause between patterns
		time.Sleep(5 * time.Second)
	}
	
	return nil
}

// PrintResults displays test results
func (lt *LoadTester) PrintResults() {
	fmt.Println("\n" + "="*60)
	fmt.Println("🏁 LOAD TEST RESULTS")
	fmt.Println("="*60)
	
	successRate := float64(lt.results.SuccessRequests) / float64(lt.results.TotalRequests) * 100
	actualRate := float64(lt.results.TotalRequests) / lt.results.TotalDuration.Seconds()
	
	fmt.Printf("Test Duration:      %v\n", lt.results.TotalDuration)
	fmt.Printf("Total Requests:     %d\n", lt.results.TotalRequests)
	fmt.Printf("Successful:         %d (%.2f%%)\n", lt.results.SuccessRequests, successRate)
	fmt.Printf("Failed:             %d\n", lt.results.FailedRequests)
	fmt.Printf("Actual Rate:        %.2f req/sec\n", actualRate)
	fmt.Printf("Target Rate:        %d req/sec\n", lt.config.RequestRate)
	fmt.Printf("Average Latency:    %v\n", lt.results.AverageLatency)
	fmt.Printf("Min Latency:        %v\n", lt.results.MinLatency)
	fmt.Printf("Max Latency:        %v\n", lt.results.MaxLatency)
	
	fmt.Println("\n📊 PERFORMANCE ASSESSMENT")
	fmt.Println("-"*40)
	
	if successRate >= 99.0 {
		fmt.Println("✅ Excellent: >99% success rate")
	} else if successRate >= 95.0 {
		fmt.Println("✅ Good: >95% success rate")
	} else if successRate >= 90.0 {
		fmt.Println("⚠️  Fair: >90% success rate")
	} else {
		fmt.Println("❌ Poor: <90% success rate")
	}
	
	if lt.results.AverageLatency < 50*time.Millisecond {
		fmt.Println("✅ Excellent: <50ms average latency")
	} else if lt.results.AverageLatency < 100*time.Millisecond {
		fmt.Println("✅ Good: <100ms average latency")
	} else if lt.results.AverageLatency < 500*time.Millisecond {
		fmt.Println("⚠️  Fair: <500ms average latency")
	} else {
		fmt.Println("❌ Poor: >500ms average latency")
	}
	
	rateAchieved := actualRate >= float64(lt.config.RequestRate)*0.9
	if rateAchieved {
		fmt.Println("✅ Rate target achieved")
	} else {
		fmt.Println("⚠️  Rate target not fully achieved")
	}
}

func main() {
	log.Println("🌊 StreamFlow Engine Demo Load Test")
	log.Println("="*50)
	
	config := LoadTestConfig{
		SimulatorURL:      "http://localhost:8000",
		PipelinesURL:      "http://localhost:8081", 
		TestDuration:      2 * time.Minute,
		ConcurrentClients: 10,
		RequestRate:       50, // 50 req/sec total
	}
	
	tester := NewLoadTester(config)
	
	// Health check first
	if err := tester.RunHealthCheck(); err != nil {
		log.Fatalf("Health check failed: %v", err)
	}
	
	log.Println("✅ All services are healthy")
	
	// Run main load test
	log.Println("\n📈 Starting main load test...")
	if err := tester.RunLoadTest(); err != nil {
		log.Fatalf("Load test failed: %v", err)
	}
	
	// Print main results
	tester.PrintResults()
	
	// Run traffic pattern test
	log.Println("\n🎭 Starting traffic pattern load test...")
	if err := tester.RunTrafficPatternTest(); err != nil {
		log.Printf("Traffic pattern test failed: %v", err)
	} else {
		log.Println("✅ Traffic pattern test completed successfully")
	}
	
	log.Println("\n🎉 Load test completed!")
}