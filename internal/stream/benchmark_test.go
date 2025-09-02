package stream

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/storage"
)

// BenchmarkEventClone benchmarks the event cloning operation
func BenchmarkEventClone(b *testing.B) {
	event := &Event{
		Key:       "benchmark-key",
		Value:     make([]byte, 1024), // 1KB payload
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
		Partition: 1,
		Offset:    1000,
		Topic:     "benchmark-topic",
		Metadata:  make(map[string]interface{}),
	}

	// Add some headers and metadata
	event.Headers["header1"] = "value1"
	event.Headers["header2"] = "value2"
	event.Metadata["meta1"] = "value1"
	event.Metadata["meta2"] = 42

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		clone := event.Clone()
		_ = clone
	}
}

// BenchmarkStreamOperations benchmarks chained stream operations
func BenchmarkStreamOperations(b *testing.B) {
	storage := &storage.Storage{} // Mock storage
	config := &ProcessorConfig{
		ProcessorName:   "benchmark-processor",
		ConsumerGroup:   "benchmark-group",
		MaxConcurrency:  4,
		BatchSize:       100,
		FlushInterval:   time.Second,
		StateStoreType:  "memory",
	}

	processor, err := NewStreamProcessor(config, storage)
	if err != nil {
		b.Fatalf("Failed to create processor: %v", err)
	}

	stream := NewStream("benchmark-topic", processor)

	// Add filter and map operations
	stream.Filter(func(event *Event) bool {
		return len(event.Key) > 5
	}).Map(func(event *Event) *Event {
		clone := event.Clone()
		clone.Key = "processed-" + clone.Key
		return clone
	})

	// Create test event
	event := &Event{
		Key:       "benchmark-key",
		Value:     []byte("benchmark-value"),
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
		Partition: 1,
		Offset:    1000,
		Topic:     "benchmark-topic",
		Metadata:  make(map[string]interface{}),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, shouldProcess := stream.applyOperations(event)
		if shouldProcess && result != nil {
			_ = result
		}
	}
}

// BenchmarkWindowManager benchmarks window creation and lookup
func BenchmarkWindowManager(b *testing.B) {
	windowSize := 5 * time.Minute
	wm := NewWindowManager(windowSize)

	baseTime := time.Now()
	timestamps := make([]time.Time, 1000)
	for i := range timestamps {
		timestamps[i] = baseTime.Add(time.Duration(i) * time.Second)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		timestamp := timestamps[i%len(timestamps)]
		window := wm.GetWindow(timestamp)
		_ = window
	}
}

// BenchmarkMemoryStateStore benchmarks memory state store operations
func BenchmarkMemoryStateStore(b *testing.B) {
	store := NewMemoryStateStore()
	ctx := context.Background()

	// Pre-populate with some data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		store.Put(ctx, key, value)
	}

	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("key-%d", i%1000)
			value, _ := store.Get(ctx, key)
			_ = value
		}
	})

	b.Run("Put", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench-key-%d", i)
			value := []byte(fmt.Sprintf("bench-value-%d", i))
			store.Put(ctx, key, value)
		}
	})

	b.Run("Delete", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench-key-%d", i)
			store.Delete(ctx, key)
		}
	})
}

// BenchmarkWindowState benchmarks window state updates
func BenchmarkWindowState(b *testing.B) {
	window := &Window{
		Start: time.Now(),
		End:   time.Now().Add(5 * time.Minute),
	}

	state := &WindowState{
		Key:       "benchmark-key",
		Window:    window,
		Count:     0,
		Sum:       0,
		Min:       0,
		Max:       0,
		Events:    make([]*Event, 0),
		Metadata:  make(map[string]interface{}),
		UpdatedAt: time.Now(),
	}

	event := &Event{
		Key:   "test-key",
		Value: []byte("42.5"),
	}
	event.SetValueFromJSON(42.5)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		state.Update(event, ValueAsFloat64)
	}
}

// BenchmarkJSON benchmarks JSON operations on events
func BenchmarkJSON(b *testing.B) {
	event := &Event{
		Key:     "benchmark-key",
		Headers: make(map[string]string),
	}

	testData := map[string]interface{}{
		"field1":    "string-value",
		"field2":    42,
		"field3":    true,
		"field4":    3.14159,
		"field5":    []string{"a", "b", "c"},
		"field6":    map[string]string{"nested": "value"},
		"timestamp": time.Now(),
	}

	b.Run("SetValueFromJSON", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			event.SetValueFromJSON(testData)
		}
	})

	event.SetValueFromJSON(testData)

	b.Run("GetValueAsJSON", func(b *testing.B) {
		var result map[string]interface{}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			event.GetValueAsJSON(&result)
		}
	})
}

// BenchmarkFilterOperations benchmarks different filter types
func BenchmarkFilterOperations(b *testing.B) {
	event := &Event{
		Key:       "test-prefix-key",
		Topic:     "test-topic",
		Timestamp: time.Now(),
	}

	b.Run("TrueFilter", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := TrueFilter(event)
			_ = result
		}
	})

	b.Run("FilterByTopic", func(b *testing.B) {
		filter := FilterByTopic("test-topic", "another-topic")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := filter(event)
			_ = result
		}
	})

	b.Run("FilterByKeyPrefix", func(b *testing.B) {
		filter := FilterByKeyPrefix("test-prefix")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := filter(event)
			_ = result
		}
	})

	b.Run("FilterByTimeRange", func(b *testing.B) {
		start := event.Timestamp.Add(-time.Hour)
		end := event.Timestamp.Add(time.Hour)
		filter := FilterByTimeRange(start, end)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := filter(event)
			_ = result
		}
	})
}

// BenchmarkMapOperations benchmarks different map operations
func BenchmarkMapOperations(b *testing.B) {
	event := &Event{
		Key:      "original-key",
		Value:    []byte("original-value"),
		Headers:  make(map[string]string),
		Metadata: make(map[string]interface{}),
	}

	b.Run("IdentityMap", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := IdentityMap(event)
			_ = result
		}
	})

	b.Run("MapAddTimestamp", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result := MapAddTimestamp(event)
			_ = result
		}
	})

	b.Run("MapAddHeader", func(b *testing.B) {
		mapper := MapAddHeader("test-header", "test-value")
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result := mapper(event)
			_ = result
		}
	})

	b.Run("MapTransformKey", func(b *testing.B) {
		transformer := MapTransformKey(func(key string) string {
			return "transformed-" + key
		})
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result := transformer(event)
			_ = result
		}
	})

	b.Run("MapTransformValue", func(b *testing.B) {
		transformer := MapTransformValue(func(value string) string {
			return "transformed-" + value
		})
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result := transformer(event)
			_ = result
		}
	})
}

// BenchmarkConcurrentAccess benchmarks concurrent access to window manager
func BenchmarkConcurrentAccess(b *testing.B) {
	windowSize := 5 * time.Minute
	wm := NewWindowManager(windowSize)

	baseTime := time.Now()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			timestamp := baseTime.Add(time.Duration(i) * time.Second)
			window := wm.GetWindow(timestamp)
			_ = window
			i++
		}
	})
}

// Performance validation test
func TestStreamProcessingPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	storage := &storage.Storage{} // Mock storage
	config := &ProcessorConfig{
		ProcessorName:   "perf-processor",
		ConsumerGroup:   "perf-group",
		MaxConcurrency:  4,
		BatchSize:       100,
		FlushInterval:   time.Second,
		StateStoreType:  "memory",
	}

	processor, err := NewStreamProcessor(config, storage)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	stream := NewStream("perf-topic", processor)

	// Add realistic processing pipeline
	processedStream := stream.
		Filter(func(event *Event) bool {
			return len(event.Value) > 0
		}).
		Map(func(event *Event) *Event {
			clone := event.Clone()
			clone.Headers["processed"] = "true"
			return clone
		})

	// Performance test: process 10,000 events
	numEvents := 10000
	events := make([]*Event, numEvents)
	for i := 0; i < numEvents; i++ {
		events[i] = &Event{
			Key:       fmt.Sprintf("key-%d", i),
			Value:     []byte(fmt.Sprintf("value-%d", i)),
			Headers:   make(map[string]string),
			Timestamp: time.Now(),
			Partition: int32(i % 4),
			Offset:    int64(i),
			Topic:     "perf-topic",
			Metadata:  make(map[string]interface{}),
		}
	}

	start := time.Now()
	processed := 0
	failed := 0

	for _, event := range events {
		// Cast to implementation to access internal method
		streamImpl, ok := processedStream.(*streamImpl)
		if !ok {
			t.Fatalf("Expected streamImpl type")
		}
		result, shouldProcess := streamImpl.applyOperations(event)
		if shouldProcess && result != nil {
			processed++
		} else {
			failed++
		}
	}

	duration := time.Since(start)
	throughput := float64(numEvents) / duration.Seconds()

	t.Logf("Performance Results:")
	t.Logf("  Events processed: %d", processed)
	t.Logf("  Events failed: %d", failed)
	t.Logf("  Total time: %v", duration)
	t.Logf("  Throughput: %.2f events/second", throughput)
	t.Logf("  Average latency per event: %v", duration/time.Duration(numEvents))

	// Performance assertions
	if throughput < 10000 { // Should process at least 10K events/second
		t.Errorf("Throughput too low: %.2f events/second (expected >= 10000)", throughput)
	}

	if processed != numEvents {
		t.Errorf("Expected to process %d events, but processed %d", numEvents, processed)
	}

	if failed > 0 {
		t.Errorf("Expected 0 failed events, but got %d", failed)
	}

	avgLatencyMicros := duration.Microseconds() / int64(numEvents)
	if avgLatencyMicros > 100 { // Should be under 100 microseconds per event
		t.Errorf("Average latency too high: %d microseconds per event (expected <= 100)", avgLatencyMicros)
	}
}

// Memory usage test
func TestMemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory test in short mode")
	}

	// Create a large number of windows and events to test memory usage
	windowSize := time.Minute
	wm := NewWindowManager(windowSize)

	baseTime := time.Now()

	// Create 1000 windows
	for i := 0; i < 1000; i++ {
		timestamp := baseTime.Add(time.Duration(i) * windowSize)
		window := wm.GetWindow(timestamp)
		_ = window
	}

	activeWindows := wm.GetActiveWindows()
	t.Logf("Created %d active windows", len(activeWindows))

	if len(activeWindows) != 1000 {
		t.Errorf("Expected 1000 active windows, got %d", len(activeWindows))
	}

	// Test state store memory usage
	store := NewMemoryStateStore()
	ctx := context.Background()

	// Store 10,000 key-value pairs
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("memory-test-key-%d", i)
		value := make([]byte, 100) // 100 bytes per value
		err := store.Put(ctx, key, value)
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Verify we can retrieve all values
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("memory-test-key-%d", i)
		value, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("Failed to get key %s: %v", key, err)
		}
		if len(value) != 100 {
			t.Errorf("Expected value length 100, got %d for key %s", len(value), key)
		}
	}

	t.Logf("Successfully stored and retrieved 10,000 key-value pairs (1MB total data)")
}