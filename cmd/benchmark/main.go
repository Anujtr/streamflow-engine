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

type BenchmarkConfig struct {
	ServerAddress string
	NumProducers  int
	NumConsumers  int
	MessageSize   int
	Duration      time.Duration
	Topic         string
}

type BenchmarkResult struct {
	Duration           time.Duration   `json:"duration"`
	MessagesProduced   int64           `json:"messages_produced"`
	MessagesConsumed   int64           `json:"messages_consumed"`
	ProducerThroughput float64         `json:"producer_throughput_msg_per_sec"`
	ConsumerThroughput float64         `json:"consumer_throughput_msg_per_sec"`
	AvgLatency         time.Duration   `json:"avg_latency_ms"`
	Errors             int64           `json:"errors"`
}

func main() {
	var (
		serverAddr    = flag.String("server", "localhost:8080", "Server address")
		numProducers  = flag.Int("producers", 4, "Number of producer goroutines")
		numConsumers  = flag.Int("consumers", 2, "Number of consumer goroutines")
		messageSize   = flag.Int("size", 1024, "Message size in bytes")
		duration      = flag.Duration("duration", 60*time.Second, "Benchmark duration")
		topic         = flag.String("topic", "benchmark-topic", "Topic name")
	)
	flag.Parse()

	config := BenchmarkConfig{
		ServerAddress: *serverAddr,
		NumProducers:  *numProducers,
		NumConsumers:  *numConsumers,
		MessageSize:   *messageSize,
		Duration:      *duration,
		Topic:         *topic,
	}

	fmt.Printf("Starting benchmark with config: %+v\n", config)

	result, err := runBenchmark(config)
	if err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	// Print results
	fmt.Println("\n=== Benchmark Results ===")
	fmt.Printf("Duration: %v\n", result.Duration)
	fmt.Printf("Messages Produced: %d\n", result.MessagesProduced)
	fmt.Printf("Messages Consumed: %d\n", result.MessagesConsumed)
	fmt.Printf("Producer Throughput: %.2f msg/sec\n", result.ProducerThroughput)
	fmt.Printf("Consumer Throughput: %.2f msg/sec\n", result.ConsumerThroughput)
	fmt.Printf("Average Latency: %v\n", result.AvgLatency)
	fmt.Printf("Errors: %d\n", result.Errors)

	// Output JSON for further analysis
	jsonResult, _ := json.MarshalIndent(result, "", "  ")
	fmt.Printf("\nJSON Result:\n%s\n", jsonResult)
}

func runBenchmark(config BenchmarkConfig) (*BenchmarkResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), config.Duration)
	defer cancel()

	var (
		totalProduced   int64
		totalConsumed   int64
		totalLatency    time.Duration
		latencyCount    int64
		totalErrors     int64
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
				Timeout: 5 * time.Second,
			})
			if err != nil {
				log.Printf("Failed to create producer %d: %v", producerID, err)
				return
			}
			defer producer.Close()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				messageStart := time.Now()
				key := fmt.Sprintf("key-%d-%d", producerID, time.Now().UnixNano())

				_, err := producer.SendSingle(ctx, config.Topic, key, payload)
				if err != nil {
					mu.Lock()
					totalErrors++
					mu.Unlock()
					continue
				}

				latency := time.Since(messageStart)

				mu.Lock()
				totalProduced++
				totalLatency += latency
				latencyCount++
				mu.Unlock()
			}
		}(i)
	}

	// Start consumers
	for i := 0; i < config.NumConsumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			consumer, err := client.NewConsumer(client.ConsumerConfig{
				Address:     config.ServerAddress,
				Timeout:     5 * time.Second,
				MaxMessages: 100,
			})
			if err != nil {
				log.Printf("Failed to create consumer %d: %v", consumerID, err)
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
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(i)
	}

	// Wait for benchmark to complete
	<-ctx.Done()
	cancel()
	wg.Wait()

	duration := time.Since(startTime)
	
	// Calculate average latency
	var avgLatency time.Duration
	if latencyCount > 0 {
		avgLatency = totalLatency / time.Duration(latencyCount)
	}

	return &BenchmarkResult{
		Duration:           duration,
		MessagesProduced:   totalProduced,
		MessagesConsumed:   totalConsumed,
		ProducerThroughput: float64(totalProduced) / duration.Seconds(),
		ConsumerThroughput: float64(totalConsumed) / duration.Seconds(),
		AvgLatency:         avgLatency,
		Errors:             totalErrors,
	}, nil
}