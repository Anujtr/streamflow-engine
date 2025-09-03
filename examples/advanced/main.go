package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/storage"
	"github.com/Anujtr/streamflow-engine/internal/stream"
)

func main() {
	// This example demonstrates StreamFlow Engine Phase 5 advanced capabilities
	// NOTE: This example uses mock components and won't run in a real distributed environment
	
	fmt.Println("=== StreamFlow Engine Phase 5 Advanced Example ===")

	// Create storage (mocked for this example)
	storage := &storage.Storage{}

	// Configure the stream processor with advanced settings
	config := &stream.ProcessorConfig{
		ProcessorName:   "advanced-processor",
		ConsumerGroup:   "advanced-group",
		MaxConcurrency:  4,
		BatchSize:       10,
		FlushInterval:   time.Second,
		StateStoreType:  "memory",
		
		// Event time configuration
		WatermarkConfig: &stream.WatermarkConfig{
			MaxOutOfOrderness: 10 * time.Second,
			IdleSourceTimeout: 30 * time.Second,
			WatermarkInterval: 5 * time.Second,
		},
		
		// Flow control configuration
		FlowControlConfig: &stream.FlowControlConfig{
			Strategy:              stream.BufferStrategy,
			BufferSize:            1000,
			BlockTimeout:          5 * time.Second,
			MaxThroughput:         1000, // events/sec
			CircuitBreakerEnabled: true,
		},
	}

	// Create stream processor
	processor, err := stream.NewStreamProcessor(config, storage)
	if err != nil {
		log.Fatalf("Failed to create stream processor: %v", err)
	}

	fmt.Printf("Created advanced stream processor: %s\n", processor.GetConfig().ProcessorName)

	// Example 1: Sliding Window Aggregation
	fmt.Println("\n--- Example 1: Sliding Window Aggregation ---")
	
	slidingStream := processor.NewStream("metrics-topic")
	
	// Create 5-minute sliding windows that slide every minute
	windowConfig := &stream.WindowConfig{
		Type:  stream.SlidingWindow,
		Size:  5 * time.Minute,
		Slide: 1 * time.Minute,
	}
	
	err = slidingStream.
		WindowConfig(windowConfig).
		Count().
		ForEach(func(result *stream.AggregateResult) {
			fmt.Printf("  Sliding Window [%s to %s]: Count=%d\n",
				result.Window.Start.Format("15:04:05"),
				result.Window.End.Format("15:04:05"),
				result.Count)
		})

	if err != nil {
		fmt.Printf("Error in sliding window processing: %v\n", err)
	}

	// Example 2: Session-based Analytics
	fmt.Println("\n--- Example 2: Session-based Analytics ---")
	
	sessionStream := processor.NewStream("user-activity-topic")
	
	err = sessionStream.
		SessionWindow(30 * time.Minute). // 30-minute session timeout
		GroupBy(func(event *stream.Event) string {
			// Group by user ID
			return event.Key
		}).
		Count().
		ForEach(func(result *stream.AggregateResult) {
			fmt.Printf("  User Session [%s]: Activity Count=%d\n",
				result.Key, result.Count)
		})

	if err != nil {
		fmt.Printf("Error in session-based processing: %v\n", err)
	}

	// Example 3: Stream Joins
	fmt.Println("\n--- Example 3: Stream Joins ---")
	
	ordersStream := processor.NewStream("orders-topic")
	paymentsStream := processor.NewStream("payments-topic")
	
	// Join orders with payments within 10-minute window
	joinFunc := func(orderEvent, paymentEvent *stream.Event) *stream.Event {
		// Create enriched event combining order and payment data
		joinedEvent := orderEvent.Clone()
		joinedEvent.Key = "order-payment-" + orderEvent.Key
		
		// Add payment info to metadata
		if joinedEvent.Metadata == nil {
			joinedEvent.Metadata = make(map[string]interface{})
		}
		joinedEvent.Metadata["payment_processed"] = true
		joinedEvent.Metadata["payment_timestamp"] = paymentEvent.Timestamp
		
		return joinedEvent
	}
	
	joinedStream := ordersStream.Join(
		paymentsStream,
		joinFunc,
		10 * time.Minute,
	)
	
	err = joinedStream.ForEach(func(result *stream.JoinResult) {
		fmt.Printf("  Joined Order-Payment: OrderKey=%s, PaymentKey=%s\n",
			result.LeftEvent.Key, result.RightEvent.Key)
	})

	if err != nil {
		fmt.Printf("Error in stream join processing: %v\n", err)
	}

	// Example 4: Complex Event Pattern Detection
	fmt.Println("\n--- Example 4: Complex Event Pattern Detection ---")
	
	fraudDetectionStream := processor.NewStream("transactions-topic")
	
	// Detect suspicious pattern: Multiple high-value transactions from same user within 5 minutes
	suspiciousPattern := func(events []*stream.Event) bool {
		if len(events) < 3 {
			return false
		}
		
		// Check if all events are from same user and have high values
		var totalAmount float64
		userID := ""
		
		for i, event := range events {
			if event.Metadata != nil {
				if uid, exists := event.Metadata["user_id"]; exists {
					currentUserID := fmt.Sprintf("%v", uid)
					if i == 0 {
						userID = currentUserID
					} else if userID != currentUserID {
						return false // Different users
					}
				}
				
				if amount, exists := event.Metadata["amount"]; exists {
					if amt, ok := amount.(float64); ok {
						totalAmount += amt
					}
				}
			}
		}
		
		// Pattern matches if same user has 3+ transactions totaling >$10,000 within timeframe
		return totalAmount > 10000.0
	}
	
	patternStream := fraudDetectionStream.Detect(suspiciousPattern, 5 * time.Minute)
	
	err = patternStream.ForEach(func(result *stream.PatternResult) {
		fmt.Printf("  🚨 FRAUD ALERT: Pattern '%s' detected with %d events\n",
			result.PatternName, len(result.Events))
		fmt.Printf("    Time Range: %s to %s\n",
			result.StartTime.Format("15:04:05"),
			result.EndTime.Format("15:04:05"))
	})

	if err != nil {
		fmt.Printf("Error in pattern detection: %v\n", err)
	}

	// Example 5: Stream Enrichment
	fmt.Println("\n--- Example 5: Stream Enrichment ---")
	
	// Mock enrichment source (would typically be a database or cache)
	enrichmentSource := &MockEnrichmentSource{
		userData: map[string]string{
			"user123": `{"name":"John Doe","tier":"premium","location":"US"}`,
			"user456": `{"name":"Jane Smith","tier":"standard","location":"EU"}`,
		},
	}
	
	enrichedStream := processor.NewStream("raw-events-topic").
		Enrich(enrichmentSource, func(event *stream.Event) string {
			// Extract user ID for enrichment lookup
			if event.Metadata != nil {
				if userID, exists := event.Metadata["user_id"]; exists {
					return fmt.Sprintf("%v", userID)
				}
			}
			return event.Key
		})
	
	err = enrichedStream.ForEach(func(event *stream.Event) {
		if event.Metadata != nil && event.Metadata["enrichment"] != nil {
			fmt.Printf("  Enriched Event: Key=%s, Enrichment=%v\n",
				event.Key, event.Metadata["enrichment"])
		}
	})

	if err != nil {
		fmt.Printf("Error in stream enrichment: %v\n", err)
	}

	// Example 6: Backpressure and Flow Control
	fmt.Println("\n--- Example 6: Backpressure and Flow Control ---")
	
	flowControlConfig := &stream.FlowControlConfig{
		Strategy:              stream.BufferStrategy,
		BufferSize:            100,
		MaxThroughput:         500, // 500 events/sec limit
		CircuitBreakerEnabled: true,
	}
	
	backpressureStream := processor.NewStream("high-volume-topic").
		WithBackpressure(flowControlConfig).
		Filter(func(event *stream.Event) bool {
			// Simulate processing that might trigger backpressure
			return event.Key != "spam"
		})
	
	err = backpressureStream.ForEach(func(event *stream.Event) {
		fmt.Printf("  Processed Event: %s (with flow control)\n", event.Key)
	})

	if err != nil {
		fmt.Printf("Error in backpressure handling: %v\n", err)
	}

	// Example 7: Event-time Processing with Watermarks
	fmt.Println("\n--- Example 7: Event-time Processing with Watermarks ---")
	
	eventTimeStream := processor.NewStream("timestamped-events-topic").
		WithEventTime(func(event *stream.Event) time.Time {
			// Extract event time from payload
			if event.Metadata != nil {
				if eventTime, exists := event.Metadata["event_time"]; exists {
					if t, ok := eventTime.(time.Time); ok {
						return t
					}
				}
			}
			return event.Timestamp // Fallback to processing time
		}).
		WithWatermark(func(event *stream.Event) *stream.Watermark {
			// Generate watermark 10 seconds behind event time
			eventTime := event.Timestamp
			if event.Metadata != nil {
				if et, exists := event.Metadata["event_time"]; exists {
					if t, ok := et.(time.Time); ok {
						eventTime = t
					}
				}
			}
			
			return &stream.Watermark{
				Timestamp: eventTime.Add(-10 * time.Second),
				Source:    "custom-watermark",
			}
		})
	
	err = eventTimeStream.
		Window(1 * time.Minute).
		Count().
		ForEach(func(result *stream.AggregateResult) {
			fmt.Printf("  Event-time Window [%s to %s]: Count=%d\n",
				result.Window.Start.Format("15:04:05"),
				result.Window.End.Format("15:04:05"),
				result.Count)
		})

	if err != nil {
		fmt.Printf("Error in event-time processing: %v\n", err)
	}

	// Example 8: Deduplication
	fmt.Println("\n--- Example 8: Event Deduplication ---")
	
	deduplicatedStream := processor.NewStream("duplicate-events-topic").
		Deduplicate(func(event *stream.Event) string {
			// Deduplicate based on message ID
			if event.Metadata != nil {
				if msgID, exists := event.Metadata["message_id"]; exists {
					return fmt.Sprintf("%v", msgID)
				}
			}
			return event.Key
		}, 5 * time.Minute)  // Deduplicate within 5-minute window
	
	err = deduplicatedStream.ForEach(func(event *stream.Event) {
		fmt.Printf("  Unique Event: %s\n", event.Key)
	})

	if err != nil {
		fmt.Printf("Error in deduplication: %v\n", err)
	}

	// Display advanced metrics
	fmt.Println("\n--- Advanced Performance Metrics ---")
	metrics := processor.GetMetrics()
	fmt.Printf("Events Processed: %d\n", metrics.EventsProcessed)
	fmt.Printf("Events Filtered: %d\n", metrics.EventsFiltered)
	fmt.Printf("Events Transformed: %d\n", metrics.EventsTransformed)
	fmt.Printf("Events Aggregated: %d\n", metrics.EventsAggregated)
	fmt.Printf("Windows Created: %d\n", metrics.WindowsCreated)
	fmt.Printf("Windows Expired: %d\n", metrics.WindowsExpired)
	fmt.Printf("Processing Errors: %d\n", metrics.ProcessingErrors)
	fmt.Printf("Average Latency: %v\n", metrics.AvgLatency)
	fmt.Printf("Throughput: %.2f events/sec\n", metrics.ThroughputPerSec)
	fmt.Printf("Health Status: %s\n", processor.GetHealthStatus())

	fmt.Println("\n=== Phase 5 Advanced Capabilities Summary ===")
	fmt.Println("StreamFlow Engine Phase 5 provides:")
	fmt.Println("✓ Advanced Windowing: Sliding and session windows")
	fmt.Println("✓ Stream Joins: Inner, left, and outer joins with time windows")
	fmt.Println("✓ Event-time Processing: Watermarks and late event handling")
	fmt.Println("✓ Complex Event Pattern Detection: Sequence, threshold, and frequency patterns")
	fmt.Println("✓ Stream Enrichment: External data source integration")
	fmt.Println("✓ Backpressure Management: Circuit breakers and adaptive flow control")
	fmt.Println("✓ Event Deduplication: Time-based duplicate detection")
	fmt.Println("✓ Advanced Time Semantics: Event-time vs processing-time")
	fmt.Println("✓ Production-ready Features: Comprehensive monitoring and fault tolerance")
	
	fmt.Println("\nNote: This example demonstrates advanced concepts with mock components.")
	fmt.Println("In a real deployment, these would integrate with actual message brokers,")
	fmt.Println("databases, and monitoring systems.")
}

// MockEnrichmentSource provides mock data for enrichment example
type MockEnrichmentSource struct {
	userData map[string]string
}

func (m *MockEnrichmentSource) Lookup(key string) ([]byte, error) {
	if data, exists := m.userData[key]; exists {
		return []byte(data), nil
	}
	return []byte(`{"error":"user not found"}`), nil
}

func (m *MockEnrichmentSource) Close() error {
	return nil
}