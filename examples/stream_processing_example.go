package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/storage"
	"github.com/Anujtr/streamflow-engine/internal/stream"
)

func main() {
	// This is a simple example demonstrating the StreamFlow Engine Phase 4 capabilities
	// NOTE: This example uses mock components and won't run in a real distributed environment
	// Real usage would require setting up gRPC servers and proper configuration

	fmt.Println("=== StreamFlow Engine Phase 4 Example ===")

	// Create storage (mocked for this example)
	storage := &storage.Storage{}

	// Configure the stream processor
	config := &stream.ProcessorConfig{
		ProcessorName:   "example-processor",
		ConsumerGroup:   "example-group",
		MaxConcurrency:  4,
		BatchSize:       10,
		FlushInterval:   time.Second,
		StateStoreType:  "memory", // Use memory state store for this example
	}

	// Create stream processor
	processor, err := stream.NewStreamProcessor(config, storage)
	if err != nil {
		log.Fatalf("Failed to create stream processor: %v", err)
	}

	fmt.Printf("Created stream processor: %s\n", processor.GetConfig().ProcessorName)

	// Example 1: Basic stream processing with filter and map
	fmt.Println("\n--- Example 1: Basic Stream Processing ---")
	basicStream := processor.NewStream("input-topic")

	// Chain operations: filter events, transform them, and log results
	processedEvents := 0
	err = basicStream.
		Filter(func(event *stream.Event) bool {
			// Only process events with keys starting with "valid"
			return len(event.Key) > 5 && event.Key[:5] == "valid"
		}).
		Map(func(event *stream.Event) *stream.Event {
			// Transform the event by adding a processed timestamp
			clone := event.Clone()
			if clone.Metadata == nil {
				clone.Metadata = make(map[string]interface{})
			}
			clone.Metadata["processed_at"] = time.Now()
			clone.Key = "processed-" + clone.Key
			return clone
		}).
		ForEach(func(event *stream.Event) {
			fmt.Printf("  Processed event: Key=%s, ProcessedAt=%v\n", 
				event.Key, event.Metadata["processed_at"])
			processedEvents++
		})

	if err != nil {
		fmt.Printf("Error in basic stream processing: %v\n", err)
	}

	// Example 2: Windowed aggregation
	fmt.Println("\n--- Example 2: Windowed Aggregation ---")
	
	// Create a windowed stream for aggregation
	windowedStream := processor.NewStream("metrics-topic").
		Window(5 * time.Minute) // 5-minute tumbling windows

	// Count events in each window
	aggregationResults := 0
	err = windowedStream.
		Count().
		ForEach(func(result *stream.AggregateResult) {
			fmt.Printf("  Window [%s to %s]: Count=%d\n",
				result.Window.Start.Format("15:04:05"),
				result.Window.End.Format("15:04:05"),
				result.Count)
			aggregationResults++
		})

	if err != nil {
		fmt.Printf("Error in windowed aggregation: %v\n", err)
	}

	// Example 3: Grouped stream processing
	fmt.Println("\n--- Example 3: Grouped Stream Processing ---")
	
	groupedStream := processor.NewStream("user-events-topic").
		GroupBy(func(event *stream.Event) string {
			// Group by user ID (assuming it's in the key)
			return event.Key
		})

	groupedResults := 0
	err = groupedStream.
		Count().
		ForEach(func(result *stream.AggregateResult) {
			fmt.Printf("  Group [%s]: Count=%d\n", result.Key, result.Count)
			groupedResults++
		})

	if err != nil {
		fmt.Printf("Error in grouped processing: %v\n", err)
	}

	// Example 4: Complex pipeline with windowed grouping
	fmt.Println("\n--- Example 4: Windowed Grouped Aggregation ---")
	
	complexResults := 0
	err = processor.NewStream("sales-topic").
		Filter(func(event *stream.Event) bool {
			// Only process sales events with positive amounts
			var saleData map[string]interface{}
			if err := event.GetValueAsJSON(&saleData); err != nil {
				return false
			}
			amount, exists := saleData["amount"]
			return exists && amount != nil
		}).
		Window(10 * time.Minute).
		GroupBy(func(event *stream.Event) string {
			// Group by product category
			var saleData map[string]interface{}
			if err := event.GetValueAsJSON(&saleData); err != nil {
				return "unknown"
			}
			category, exists := saleData["category"]
			if !exists {
				return "unknown"
			}
			return fmt.Sprintf("%v", category)
		}).
		Sum(func(event *stream.Event) (float64, error) {
			// Sum the sale amounts
			var saleData map[string]interface{}
			if err := event.GetValueAsJSON(&saleData); err != nil {
				return 0, err
			}
			amount, exists := saleData["amount"]
			if !exists {
				return 0, fmt.Errorf("no amount field")
			}
			switch v := amount.(type) {
			case float64:
				return v, nil
			case int:
				return float64(v), nil
			default:
				return 0, fmt.Errorf("amount is not a number")
			}
		}).
		ForEach(func(result *stream.AggregateResult) {
			fmt.Printf("  Category [%s] in window [%s to %s]: Total Sales=$%.2f\n",
				result.Key,
				result.Window.Start.Format("15:04:05"),
				result.Window.End.Format("15:04:05"),
				result.Sum)
			complexResults++
		})

	if err != nil {
		fmt.Printf("Error in complex pipeline: %v\n", err)
	}

	// Display metrics
	fmt.Println("\n--- Performance Metrics ---")
	metrics := processor.GetMetrics()
	fmt.Printf("Events Processed: %d\n", metrics.EventsProcessed)
	fmt.Printf("Events Filtered: %d\n", metrics.EventsFiltered)
	fmt.Printf("Events Transformed: %d\n", metrics.EventsTransformed)
	fmt.Printf("Processing Errors: %d\n", metrics.ProcessingErrors)
	fmt.Printf("Average Latency: %v\n", metrics.AvgLatency)
	fmt.Printf("Health Status: %s\n", processor.GetHealthStatus())

	// Demonstrate state store capabilities
	fmt.Println("\n--- State Store Capabilities ---")
	stateStore := processor.GetStateStore()
	
	ctx := context.Background()
	
	// Store some application state
	err = stateStore.Put(ctx, "app-config", []byte(`{"version":"1.0","features":["windowing","aggregation"]}`))
	if err != nil {
		fmt.Printf("Error storing state: %v\n", err)
	} else {
		fmt.Println("  Stored application configuration in state store")
	}

	// Retrieve state
	configData, err := stateStore.Get(ctx, "app-config")
	if err != nil {
		fmt.Printf("Error retrieving state: %v\n", err)
	} else {
		fmt.Printf("  Retrieved configuration: %s\n", string(configData))
	}

	fmt.Println("\n=== Summary ===")
	fmt.Println("StreamFlow Engine Phase 4 provides:")
	fmt.Println("✓ Fluent API for stream processing")
	fmt.Println("✓ Filter and Map operations")
	fmt.Println("✓ Time-based windowing (tumbling windows)")
	fmt.Println("✓ Grouping and aggregation (Count, Sum, Average, Min, Max)")
	fmt.Println("✓ Stateful processing with persistent state stores")
	fmt.Println("✓ High-performance event processing")
	fmt.Println("✓ Integration with existing consumer/producer infrastructure")
	fmt.Println("✓ Comprehensive metrics and monitoring")
	
	fmt.Println("\nNote: This example uses mocked components. In a real deployment,")
	fmt.Println("you would configure actual gRPC servers and storage backends.")
}