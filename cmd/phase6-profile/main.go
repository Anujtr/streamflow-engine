package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/metrics"
	"github.com/Anujtr/streamflow-engine/internal/storage"
	"github.com/Anujtr/streamflow-engine/internal/stream"
)

const (
	NumMessages     = 100000
	NumProducers    = 4
	NumConsumers    = 2
	ProcessorBatch  = 1000
)

func main() {
	log.Printf("Starting Phase 6 performance profiling...")
	log.Printf("Target: %d messages, %d producers, %d consumers", NumMessages, NumProducers, NumConsumers)

	// Enable CPU profiling
	cpuFile, err := os.Create("cpu_profile.prof")
	if err != nil {
		log.Fatalf("Failed to create CPU profile: %v", err)
	}
	defer cpuFile.Close()

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		log.Fatalf("Failed to start CPU profile: %v", err)
	}
	defer pprof.StopCPUProfile()

	// Create storage and metrics
	store := storage.NewStorage()
	perfMetrics := metrics.NewPerformanceMetrics()
	
	// Create topic
	topicName := "profile-topic"
	if err := store.CreateTopic(topicName, 8); err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	// Create stream processor
	processorConfig := &stream.ProcessorConfig{
		ProcessorName:   "profile-processor",
		ConsumerGroup:   "profile-group",
		MaxConcurrency:  4,
		BatchSize:       ProcessorBatch,
		FlushInterval:   time.Millisecond * 100,
		StateStoreType:  "memory",
	}

	processor, err := stream.NewStreamProcessor(processorConfig, store)
	if err != nil {
		log.Fatalf("Failed to create stream processor: %v", err)
	}

	// Start performance test
	startTime := time.Now()
	
	// Phase 1: Raw produce/consume performance
	log.Println("Phase 1: Testing raw produce/consume performance...")
	testRawPerformance(store, perfMetrics, topicName)

	// Phase 2: Stream processing performance
	log.Println("Phase 2: Testing stream processing performance...")
	testStreamProcessingPerformance(processor, store, topicName)

	// Phase 3: Complex pipeline performance  
	log.Println("Phase 3: Testing complex pipeline performance...")
	testComplexPipelinePerformance(processor, store, topicName)

	totalDuration := time.Since(startTime)

	// Memory profiling
	memFile, err := os.Create("mem_profile.prof")
	if err != nil {
		log.Fatalf("Failed to create memory profile: %v", err)
	}
	defer memFile.Close()

	runtime.GC()
	if err := pprof.WriteHeapProfile(memFile); err != nil {
		log.Fatalf("Failed to write memory profile: %v", err)
	}

	// Report results
	log.Printf("=== PHASE 6 PROFILING RESULTS ===")
	log.Printf("Total Duration: %v", totalDuration)
	log.Printf("CPU Profile: cpu_profile.prof")
	log.Printf("Memory Profile: mem_profile.prof")
	log.Printf("Use: go tool pprof cpu_profile.prof")
	log.Printf("Use: go tool pprof mem_profile.prof")
	
	// Print memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("Memory Stats:")
	log.Printf("  Allocated: %d KB", m.Alloc/1024)
	log.Printf("  Total Allocated: %d KB", m.TotalAlloc/1024)
	log.Printf("  System: %d KB", m.Sys/1024)
	log.Printf("  GC Cycles: %d", m.NumGC)

	processor.Stop()
	log.Println("Profiling completed successfully!")
}

func testRawPerformance(store *storage.Storage, perfMetrics *metrics.PerformanceMetrics, topicName string) {
	var wg sync.WaitGroup
	messages := make([]*storage.Message, NumMessages)
	
	// Prepare messages
	for i := 0; i < NumMessages; i++ {
		messages[i] = &storage.Message{
			Key:       fmt.Sprintf("key-%d", i),
			Value:     []byte(fmt.Sprintf("test-message-%d-with-some-payload-data", i)),
			Timestamp: time.Now(),
		}
	}

	startTime := time.Now()

	// Start producers
	wg.Add(NumProducers)
	for p := 0; p < NumProducers; p++ {
		go func(producerID int) {
			defer wg.Done()
			start := producerID * NumMessages / NumProducers
			end := (producerID + 1) * NumMessages / NumProducers
			
			for i := start; i < end; i++ {
				_, _, err := store.Produce(topicName, messages[i])
				if err != nil {
					log.Printf("Producer %d error: %v", producerID, err)
					perfMetrics.IncrementProduceErrors()
				} else {
					perfMetrics.RecordProduceLatency(time.Since(startTime))
				}
			}
		}(p)
	}

	wg.Wait()
	produceDuration := time.Since(startTime)

	// Start consumers
	consumeStart := time.Now()
	wg.Add(NumConsumers)
	consumedCount := int64(0)
	
	for c := 0; c < NumConsumers; c++ {
		go func(consumerID int) {
			defer wg.Done()
			partition := int32(consumerID % 8) // Assuming 8 partitions
			offset := int64(0)
			
			for {
				messages, hasMore, err := store.Consume(topicName, partition, offset, 100)
				if err != nil {
					log.Printf("Consumer %d error: %v", consumerID, err)
					perfMetrics.IncrementConsumeErrors()
					break
				}
				
				if len(messages) > 0 {
					consumedCount += int64(len(messages))
					offset += int64(len(messages))
					perfMetrics.RecordConsumeLatency(time.Since(consumeStart), len(messages))
				}
				
				if !hasMore || consumedCount >= NumMessages/2 {
					break
				}
				
				time.Sleep(time.Millisecond * 10)
			}
		}(c)
	}

	wg.Wait()
	consumeDuration := time.Since(consumeStart)

	log.Printf("Raw Performance Results:")
	log.Printf("  Produce: %d msg/sec", int64(float64(NumMessages)/produceDuration.Seconds()))
	log.Printf("  Consume: %d msg/sec", int64(float64(consumedCount)/consumeDuration.Seconds()))
	log.Printf("  Produce Duration: %v", produceDuration)
	log.Printf("  Consume Duration: %v", consumeDuration)
}

func testStreamProcessingPerformance(processor stream.StreamProcessor, store *storage.Storage, topicName string) {
	processingStart := time.Now()
	eventsProcessed := int64(0)
	
	// Create a complex stream processing pipeline
	processor.NewStream(topicName).
		Filter(func(event *stream.Event) bool {
			eventsProcessed++
			return len(event.Value) > 10 // Simple filter
		}).
		Map(func(event *stream.Event) *stream.Event {
			// Simple transformation
			clone := event.Clone()
			clone.Key = "processed-" + clone.Key
			return clone
		}).
		Window(time.Second).
		Count().
		ForEach(func(result *stream.AggregateResult) {
			// Process aggregate result
			_ = result
		})

	// Feed data to stream processor
	for i := 0; i < 10000; i++ {
		msg := &storage.Message{
			Key:       fmt.Sprintf("stream-key-%d", i),
			Value:     []byte(fmt.Sprintf("stream-message-%d-for-processing", i)),
			Timestamp: time.Now(),
		}
		store.Produce(topicName, msg)
	}

	// Let processing run for a bit
	time.Sleep(time.Second * 3)

	processingDuration := time.Since(processingStart)
	
	// Get processor metrics
	procMetrics := processor.GetMetrics()
	
	log.Printf("Stream Processing Results:")
	log.Printf("  Events Processed: %d", procMetrics.EventsProcessed)
	log.Printf("  Events Filtered: %d", procMetrics.EventsFiltered)
	log.Printf("  Events Transformed: %d", procMetrics.EventsTransformed)
	log.Printf("  Windows Created: %d", procMetrics.WindowsCreated)
	log.Printf("  Processing Duration: %v", processingDuration)
	log.Printf("  Throughput: %.2f events/sec", procMetrics.ThroughputPerSec)
	log.Printf("  Average Latency: %v", procMetrics.AvgLatency)
}

func testComplexPipelinePerformance(processor stream.StreamProcessor, store *storage.Storage, topicName string) {
	complexStart := time.Now()
	
	// Test advanced Phase 5 features
	processor.NewStream(topicName).
		Filter(func(event *stream.Event) bool {
			return event.Key != "ignore"
		}).
		Map(func(event *stream.Event) *stream.Event {
			clone := event.Clone()
			if clone.Metadata == nil {
				clone.Metadata = make(map[string]interface{})
			}
			clone.Metadata["processing_time"] = time.Now()
			return clone
		}).
		WindowConfig(&stream.WindowConfig{
			Type:  stream.SlidingWindow,
			Size:  time.Second * 5,
			Slide: time.Second * 1,
		}).
		GroupBy(func(event *stream.Event) string {
			// Group by key prefix
			if len(event.Key) > 5 {
				return event.Key[:5]
			}
			return event.Key
		}).
		Sum(func(event *stream.Event) (float64, error) {
			return 1.0, nil // Simple counting
		}).
		Output("complex-output-topic")

	// Generate complex test data
	for i := 0; i < 5000; i++ {
		msg := &storage.Message{
			Key:       fmt.Sprintf("complex-%d", i%100), // Create groups
			Value:     []byte(fmt.Sprintf("complex-data-%d", i)),
			Timestamp: time.Now(),
		}
		store.Produce(topicName, msg)
	}

	// Let complex processing run
	time.Sleep(time.Second * 5)

	complexDuration := time.Since(complexStart)
	procMetrics := processor.GetMetrics()

	log.Printf("Complex Pipeline Results:")
	log.Printf("  Duration: %v", complexDuration)
	log.Printf("  Events Processed: %d", procMetrics.EventsProcessed)
	log.Printf("  Windows Created: %d", procMetrics.WindowsCreated)
	log.Printf("  Windows Expired: %d", procMetrics.WindowsExpired)
	log.Printf("  Throughput: %.2f events/sec", procMetrics.ThroughputPerSec)
}