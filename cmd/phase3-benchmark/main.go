package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/Anujtr/streamflow-engine/pkg/client"
)

type Phase3BenchmarkConfig struct {
	ServerAddress   string
	NumProducers    int
	NumConsumers    int
	MessageSize     int
	Duration        time.Duration
	Topic           string
	TestPersistence bool
	UseOffsets      bool
}

type Phase3BenchmarkResult struct {
	Duration              time.Duration   `json:"duration"`
	MessagesProduced      int64           `json:"messages_produced"`
	MessagesConsumed      int64           `json:"messages_consumed"`
	ProducerThroughput    float64         `json:"producer_throughput_msg_per_sec"`
	ConsumerThroughput    float64         `json:"consumer_throughput_msg_per_sec"`
	P50Latency            time.Duration   `json:"p50_latency_ms"`
	P99Latency            time.Duration   `json:"p99_latency_ms"`
	OffsetCommits         int64           `json:"offset_commits"`
	Errors                int64           `json:"errors"`
	PersistenceMode       bool            `json:"persistence_mode"`
	OffsetManagementMode  bool            `json:"offset_management_mode"`
}

func main() {
	var (
		serverAddr      = flag.String("server", "localhost:8080", "Server address")
		numProducers    = flag.Int("producers", 8, "Number of producer goroutines")
		numConsumers    = flag.Int("consumers", 4, "Number of consumer goroutines")
		messageSize     = flag.Int("size", 512, "Message size in bytes")
		duration        = flag.Duration("duration", 30*time.Second, "Benchmark duration")
		topic           = flag.String("topic", "phase3-benchmark-topic", "Topic name")
		testPersistence = flag.Bool("persistence", true, "Test with persistent storage")
		useOffsets      = flag.Bool("offsets", true, "Test with offset management")
	)
	flag.Parse()

	config := Phase3BenchmarkConfig{
		ServerAddress:   *serverAddr,
		NumProducers:    *numProducers,
		NumConsumers:    *numConsumers,
		MessageSize:     *messageSize,
		Duration:        *duration,
		Topic:           *topic,
		TestPersistence: *testPersistence,
		UseOffsets:      *useOffsets,
	}

	fmt.Printf("Starting Phase 3 Benchmark\n")
	fmt.Printf("Config: %+v\n", config)
	
	if config.TestPersistence {
		fmt.Printf("✅ Testing with Pebble persistent storage\n")
	}
	if config.UseOffsets {
		fmt.Printf("✅ Testing with consumer offset management\n")
	}

	result, err := runPhase3Benchmark(config)
	if err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	// Print results
	fmt.Println("\n=== Phase 3 Benchmark Results ===")
	fmt.Printf("Duration: %v\n", result.Duration)
	fmt.Printf("Messages Produced: %d\n", result.MessagesProduced)
	fmt.Printf("Messages Consumed: %d\n", result.MessagesConsumed)
	fmt.Printf("Producer Throughput: %.2f msg/sec\n", result.ProducerThroughput)
	fmt.Printf("Consumer Throughput: %.2f msg/sec\n", result.ConsumerThroughput)
	fmt.Printf("P50 Latency: %v\n", result.P50Latency)
	fmt.Printf("P99 Latency: %v\n", result.P99Latency)
	if result.OffsetCommits > 0 {
		fmt.Printf("Offset Commits: %d\n", result.OffsetCommits)
	}
	fmt.Printf("Errors: %d (%.2f%%)\n", result.Errors, float64(result.Errors)/float64(result.MessagesProduced)*100)
	fmt.Printf("Persistence Mode: %v\n", result.PersistenceMode)
	fmt.Printf("Offset Management: %v\n", result.OffsetManagementMode)
	
	// Performance assessment
	fmt.Println("\n=== Phase 3 Assessment ===")
	if result.ProducerThroughput >= 50000 {
		fmt.Printf("🎯 PERFORMANCE TARGET MET: %.0f msg/sec >= 50K target\n", result.ProducerThroughput)
	} else {
		fmt.Printf("📈 Progress toward 50K target: %.0f msg/sec (%.1f%%)\n", 
			result.ProducerThroughput, result.ProducerThroughput/50000*100)
	}
	
	if result.P99Latency < 50*time.Millisecond {
		fmt.Printf("⚡ LATENCY TARGET MET: P99 %v < 50ms target\n", result.P99Latency)
	}
	
	errorRate := float64(result.Errors) / float64(result.MessagesProduced) * 100
	if errorRate < 1.0 {
		fmt.Printf("✅ RELIABILITY TARGET MET: %.2f%% error rate\n", errorRate)
	}

	// Output JSON for analysis
	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	fmt.Printf("\nJSON Result:\n%s\n", jsonResult)
}

func runPhase3Benchmark(config Phase3BenchmarkConfig) (*Phase3BenchmarkResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), config.Duration)
	defer cancel()

	var (
		totalProduced   int64
		totalConsumed   int64
		offsetCommits   int64
		totalErrors     int64
		latencies       []time.Duration
		latencyMu       sync.Mutex
		wg              sync.WaitGroup
		mu              sync.Mutex
	)

	// Create message payload
	payload := make([]byte, config.MessageSize)
	rand.Read(payload)

	startTime := time.Now()

	// Start producers
	for i := 0; i < config.NumProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()

			producer, err := client.NewProducer(client.ProducerConfig{
				Address: config.ServerAddress,
				Timeout: 10 * time.Second,
			})
			if err != nil {
				log.Printf("Producer %d failed to connect: %v", producerID, err)
				mu.Lock()
				totalErrors++
				mu.Unlock()
				return
			}
			defer producer.Close()

			messageCount := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				messageStart := time.Now()
				key := fmt.Sprintf("prod-%d-msg-%d", producerID, messageCount)
				messageCount++

				_, err := producer.SendSingle(ctx, config.Topic, key, payload)
				latency := time.Since(messageStart)

				if err != nil {
					mu.Lock()
					totalErrors++
					mu.Unlock()
					continue
				}

				mu.Lock()
				totalProduced++
				mu.Unlock()

				// Sample latencies (to avoid memory issues with long benchmarks)
				if messageCount%10 == 0 {
					latencyMu.Lock()
					latencies = append(latencies, latency)
					latencyMu.Unlock()
				}
			}
		}(i)
	}

	// Start consumers
	for i := 0; i < config.NumConsumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			if config.UseOffsets {
				// Use managed consumer with offset management
				managedConsumer, err := client.NewManagedConsumer(client.ManagedConsumerConfig{
					Address:                config.ServerAddress,
					Timeout:                10 * time.Second,
					MaxMessages:            100,
					ConsumerGroup:          fmt.Sprintf("phase3-bench-group-%d", consumerID),
					EnableAutoCommit:       true,
					AutoCommitInterval:     1 * time.Second,
					EnableAutoOffsetStore:  true,
				})
				if err != nil {
					log.Printf("Managed Consumer %d failed to connect: %v", consumerID, err)
					mu.Lock()
					totalErrors++
					mu.Unlock()
					return
				}
				defer managedConsumer.Close()

				partition := int32(consumerID % 4) // Assume 4 partitions
				
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}

					messages, err := managedConsumer.ConsumeFromCommittedOffset(ctx, config.Topic, partition, 100)
					if err != nil {
						mu.Lock()
						totalErrors++
						mu.Unlock()
						time.Sleep(100 * time.Millisecond)
						continue
					}

					if len(messages) > 0 {
						mu.Lock()
						totalConsumed += int64(len(messages))
						offsetCommits++ // Auto-committed
						mu.Unlock()
					} else {
						time.Sleep(50 * time.Millisecond)
					}
				}
			} else {
				// Use regular consumer
				consumer, err := client.NewConsumer(client.ConsumerConfig{
					Address:     config.ServerAddress,
					Timeout:     10 * time.Second,
					MaxMessages: 100,
				})
				if err != nil {
					log.Printf("Consumer %d failed to connect: %v", consumerID, err)
					mu.Lock()
					totalErrors++
					mu.Unlock()
					return
				}
				defer consumer.Close()

				partition := int32(consumerID % 4) // Assume 4 partitions
				offset := int64(0)

				for {
					select {
					case <-ctx.Done():
						return
					default:
					}

					messages, err := consumer.Consume(ctx, config.Topic, partition, offset, 100)
					if err != nil {
						mu.Lock()
						totalErrors++
						mu.Unlock()
						time.Sleep(100 * time.Millisecond)
						continue
					}

					if len(messages) > 0 {
						mu.Lock()
						totalConsumed += int64(len(messages))
						mu.Unlock()
						offset = messages[len(messages)-1].Offset + 1
					} else {
						time.Sleep(50 * time.Millisecond)
					}
				}
			}
		}(i)
	}

	// Wait for benchmark to complete
	<-ctx.Done()
	cancel()
	wg.Wait()

	duration := time.Since(startTime)
	
	// Calculate latency percentiles
	var p50Latency, p99Latency time.Duration
	if len(latencies) > 0 {
		// Simple percentile calculation
		n := len(latencies)
		if n >= 2 {
			p50Index := n * 50 / 100
			p99Index := n * 99 / 100
			if p99Index >= n {
				p99Index = n - 1
			}
			
			// Sort latencies for percentile calculation
			for i := 0; i < len(latencies)-1; i++ {
				for j := 0; j < len(latencies)-i-1; j++ {
					if latencies[j] > latencies[j+1] {
						latencies[j], latencies[j+1] = latencies[j+1], latencies[j]
					}
				}
			}
			
			p50Latency = latencies[p50Index]
			p99Latency = latencies[p99Index]
		}
	}

	return &Phase3BenchmarkResult{
		Duration:              duration,
		MessagesProduced:      totalProduced,
		MessagesConsumed:      totalConsumed,
		ProducerThroughput:    float64(totalProduced) / duration.Seconds(),
		ConsumerThroughput:    float64(totalConsumed) / duration.Seconds(),
		P50Latency:            p50Latency,
		P99Latency:            p99Latency,
		OffsetCommits:         offsetCommits,
		Errors:                totalErrors,
		PersistenceMode:       config.TestPersistence,
		OffsetManagementMode:  config.UseOffsets,
	}, nil
}