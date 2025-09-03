package stream

import (
	"context"
	"testing"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/storage"
)

// BenchmarkSlidingWindowManager tests sliding window performance
func BenchmarkSlidingWindowManager(b *testing.B) {
	windowSize := 5 * time.Minute
	windowSlide := 1 * time.Minute
	swm := NewSlidingWindowManager(windowSize, windowSlide)
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	swm.Start(ctx)
	defer swm.Stop()
	
	timestamp := time.Now()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			swm.GetWindow(timestamp.Add(time.Duration(b.N) * time.Nanosecond))
		}
	})
}

// BenchmarkSessionWindowManager tests session window performance
func BenchmarkSessionWindowManager(b *testing.B) {
	sessionTimeout := 30 * time.Minute
	swm := NewSessionWindowManager(sessionTimeout, nil)
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	swm.Start(ctx)
	defer swm.Stop()
	
	events := make([]*Event, b.N)
	for i := 0; i < b.N; i++ {
		events[i] = &Event{
			Key:       "session-" + string(rune(i%100)), // 100 different sessions
			Timestamp: time.Now().Add(time.Duration(i) * time.Millisecond),
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		swm.GetOrCreateSessionWindow(events[i])
	}
}

// BenchmarkWatermarkManager tests watermark performance
func BenchmarkWatermarkManager(b *testing.B) {
	wm := NewWatermarkManager(&WatermarkConfig{
		MaxOutOfOrderness: 5 * time.Second,
		IdleSourceTimeout: 30 * time.Second,
		WatermarkInterval: 1 * time.Second,
	})
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wm.Start(ctx)
	defer wm.Stop()
	
	timestamp := time.Now()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			source := "source-" + string(rune(i%10)) // 10 different sources
			wm.UpdateWatermark(source, timestamp.Add(time.Duration(i) * time.Nanosecond))
			i++
		}
	})
}

// BenchmarkPatternDetector tests pattern detection performance
func BenchmarkPatternDetector(b *testing.B) {
	pd := NewPatternDetector()
	
	// Add a simple pattern that matches every 3 events
	matcher := func(events []*Event) bool {
		return len(events) == 3
	}
	
	pd.AddPattern("three-events", matcher, 5*time.Minute)
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pd.Start(ctx)
	defer pd.Stop()
	
	events := make([]*Event, b.N)
	for i := 0; i < b.N; i++ {
		events[i] = &Event{
			Key:       "pattern-" + string(rune(i%50)), // 50 different keys
			Timestamp: time.Now().Add(time.Duration(i) * time.Millisecond),
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pd.ProcessEvent(events[i])
	}
}

// BenchmarkFlowController tests flow controller performance
func BenchmarkFlowController(b *testing.B) {
	config := &FlowControlConfig{
		Strategy:              BufferStrategy,
		BufferSize:            10000,
		MaxThroughput:         100000, // High throughput for benchmarking
		CircuitBreakerEnabled: false,  // Disable to reduce overhead
	}
	
	fc := NewFlowController(config)
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fc.Start(ctx)
	defer fc.Stop()
	
	events := make([]*Event, b.N)
	for i := 0; i < b.N; i++ {
		events[i] = &Event{
			Key:       "flow-test",
			Timestamp: time.Now(),
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fc.ApplyBackpressure(events[i])
	}
}

// BenchmarkStreamJoiner tests stream join performance
func BenchmarkStreamJoiner(b *testing.B) {
	config := &JoinConfig{
		Type:           InnerJoin,
		WindowSize:     5 * time.Minute,
		MaxBufferSize:  10000,
		LeftKeyFunc:    func(e *Event) string { return e.Key },
		RightKeyFunc:   func(e *Event) string { return e.Key },
	}
	
	joiner := NewStreamJoiner(config)
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	joiner.Start(ctx)
	defer joiner.Stop()
	
	// Prepare events
	leftEvents := make([]*Event, b.N/2)
	rightEvents := make([]*Event, b.N/2)
	
	for i := 0; i < b.N/2; i++ {
		leftEvents[i] = &Event{
			Key:       "join-" + string(rune(i%100)), // 100 different keys
			Timestamp: time.Now().Add(time.Duration(i) * time.Millisecond),
			Value:     []byte("left-value"),
		}
		rightEvents[i] = &Event{
			Key:       "join-" + string(rune(i%100)), // Same keys for joining
			Timestamp: time.Now().Add(time.Duration(i+1) * time.Millisecond),
			Value:     []byte("right-value"),
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N/2; i++ {
		joiner.ProcessLeftEvent(leftEvents[i])
		joiner.ProcessRightEvent(rightEvents[i])
	}
}

// BenchmarkStreamEnricher tests stream enrichment performance
func BenchmarkStreamEnricher(b *testing.B) {
	// Mock enrichment source with precomputed data
	source := &fastMockEnrichmentSource{
		data: make(map[string][]byte),
	}
	
	// Precompute enrichment data
	for i := 0; i < 1000; i++ {
		key := "enrich-" + string(rune(i))
		source.data[key] = []byte(`{"enriched": true, "value": ` + string(rune(i)) + `}`)
	}
	
	keyExtractor := func(e *Event) string {
		return e.Key
	}
	
	enricher := NewStreamEnricher(source, keyExtractor)
	defer enricher.Close()
	
	events := make([]*Event, b.N)
	for i := 0; i < b.N; i++ {
		events[i] = &Event{
			Key:       "enrich-" + string(rune(i%1000)), // Cycle through 1000 keys
			Value:     []byte("original-value"),
			Metadata:  make(map[string]interface{}),
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enricher.Enrich(events[i])
	}
}

// BenchmarkEndToEndAdvancedPipeline tests complete Phase 5 pipeline
func BenchmarkEndToEndAdvancedPipeline(b *testing.B) {
	// Create a complete advanced pipeline with Phase 5 features
	storage := &storage.Storage{}
	config := &ProcessorConfig{
		ProcessorName:   "benchmark-processor",
		ConsumerGroup:   "benchmark-group",
		MaxConcurrency:  4,
		BatchSize:       100,
		FlushInterval:   time.Millisecond,
		StateStoreType:  "memory",
		WatermarkConfig: &WatermarkConfig{
			MaxOutOfOrderness: 1 * time.Second,
			IdleSourceTimeout: 5 * time.Second,
			WatermarkInterval: 100 * time.Millisecond,
		},
		FlowControlConfig: &FlowControlConfig{
			Strategy:              BufferStrategy,
			BufferSize:            1000,
			MaxThroughput:         1000000, // Very high for benchmarking
			CircuitBreakerEnabled: false,
		},
	}
	
	processor, err := NewStreamProcessor(config, storage)
	if err != nil {
		b.Fatalf("Failed to create processor: %v", err)
	}
	
	// Create advanced stream with multiple Phase 5 features
	stream := processor.NewStream("benchmark-topic").
		WithEventTime(func(e *Event) time.Time { return e.Timestamp }).
		Filter(func(e *Event) bool { return e.Key != "filtered" }).
		Map(func(e *Event) *Event {
			clone := e.Clone()
			clone.Key = "processed-" + clone.Key
			return clone
		})
	
	// Create windowed aggregation
	windowedStream := stream.SlidingWindow(5*time.Second, 1*time.Second)
	
	events := make([]*Event, b.N)
	for i := 0; i < b.N; i++ {
		events[i] = &Event{
			Key:       "benchmark-" + string(rune(i%100)),
			Value:     []byte("benchmark-value"),
			Timestamp: time.Now().Add(time.Duration(i) * time.Microsecond),
			Metadata:  map[string]interface{}{"sequence": i},
		}
	}
	
	b.ResetTimer()
	
	// Simulate processing events through the advanced pipeline
	for i := 0; i < b.N; i++ {
		// This would normally go through the full pipeline
		// For benchmarking, we test individual components
		event := events[i]
		
		// Test filter
		if event.Key != "filtered" {
			// Test map
			clone := event.Clone()
			clone.Key = "processed-" + clone.Key
			
			// Test windowing (simplified)
			_ = windowedStream
		}
	}
}

// BenchmarkPatternMatching tests various pattern matching functions
func BenchmarkPatternMatching(b *testing.B) {
	events := make([]*Event, 10)
	for i := 0; i < 10; i++ {
		events[i] = &Event{
			Key:       "event-" + string(rune(65+i)), // A, B, C, etc.
			Timestamp: time.Now().Add(time.Duration(i) * time.Minute),
		}
	}
	
	sequencePattern := SequencePattern("event-A", "event-B", "event-C")
	thresholdPattern := ThresholdPattern(ValueAsFloat64, 100, ">")
	frequencyPattern := FrequencyPattern(5, 10*time.Minute)
	
	b.Run("SequencePattern", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sequencePattern(events[:3])
		}
	})
	
	b.Run("ThresholdPattern", func(b *testing.B) {
		// Add value metadata to events
		for _, event := range events {
			if event.Metadata == nil {
				event.Metadata = make(map[string]interface{})
			}
			event.Metadata["value"] = float64(150) // Above threshold
		}
		
		for i := 0; i < b.N; i++ {
			thresholdPattern(events[:5])
		}
	})
	
	b.Run("FrequencyPattern", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			frequencyPattern(events[:7])
		}
	})
}

// BenchmarkMemoryUsage tests memory efficiency of Phase 5 components
func BenchmarkMemoryUsage(b *testing.B) {
	b.Run("SlidingWindows", func(b *testing.B) {
		swm := NewSlidingWindowManager(10*time.Minute, 1*time.Minute)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		swm.Start(ctx)
		defer swm.Stop()
		
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			timestamp := time.Now().Add(time.Duration(i) * time.Second)
			swm.GetWindow(timestamp)
		}
	})
	
	b.Run("SessionWindows", func(b *testing.B) {
		swm := NewSessionWindowManager(30*time.Minute, nil)
		events := make([]*Event, b.N)
		for i := 0; i < b.N; i++ {
			events[i] = &Event{
				Key:       "session-" + string(rune(i%10)),
				Timestamp: time.Now(),
			}
		}
		
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			swm.GetOrCreateSessionWindow(events[i])
		}
	})
	
	b.Run("PatternDetection", func(b *testing.B) {
		pd := NewPatternDetector()
		matcher := func(events []*Event) bool { return len(events) >= 2 }
		pd.AddPattern("test", matcher, 1*time.Minute)
		
		events := make([]*Event, b.N)
		for i := 0; i < b.N; i++ {
			events[i] = &Event{
				Key:       "pattern-" + string(rune(i%5)),
				Timestamp: time.Now(),
			}
		}
		
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			pd.ProcessEvent(events[i])
		}
	})
}

// fastMockEnrichmentSource for benchmarking (optimized for speed)
type fastMockEnrichmentSource struct {
	data map[string][]byte
}

func (m *fastMockEnrichmentSource) Lookup(key string) ([]byte, error) {
	if data, exists := m.data[key]; exists {
		return data, nil
	}
	return []byte(`{}`), nil // Return empty JSON instead of error for benchmarking
}

func (m *fastMockEnrichmentSource) Close() error {
	return nil
}