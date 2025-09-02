package stream

import (
	"context"
	"testing"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/storage"
)

func TestNewWindowManager(t *testing.T) {
	windowSize := 5 * time.Minute
	wm := NewWindowManager(windowSize)

	if wm == nil {
		t.Fatal("NewWindowManager returned nil")
	}

	if wm.GetWindowSize() != windowSize {
		t.Errorf("Expected window size %v, got %v", windowSize, wm.GetWindowSize())
	}

	if wm.GetWindowSlide() != windowSize {
		t.Errorf("Expected window slide %v, got %v", windowSize, wm.GetWindowSlide())
	}

	activeWindows := wm.GetActiveWindows()
	if len(activeWindows) != 0 {
		t.Errorf("Expected 0 active windows, got %d", len(activeWindows))
	}
}

func TestWindowManager_GetWindow(t *testing.T) {
	windowSize := 5 * time.Minute
	wm := NewWindowManager(windowSize)

	// Test creating window for a specific timestamp
	timestamp := time.Date(2024, 1, 1, 12, 7, 30, 0, time.UTC)
	window := wm.GetWindow(timestamp)

	if window == nil {
		t.Fatal("GetWindow returned nil")
	}

	// Window should start at the truncated time
	expectedStart := timestamp.Truncate(windowSize)
	expectedEnd := expectedStart.Add(windowSize)

	if !window.Start.Equal(expectedStart) {
		t.Errorf("Expected window start %v, got %v", expectedStart, window.Start)
	}

	if !window.End.Equal(expectedEnd) {
		t.Errorf("Expected window end %v, got %v", expectedEnd, window.End)
	}

	// Getting the same window again should return the same instance
	window2 := wm.GetWindow(timestamp)
	if window != window2 {
		t.Error("Getting the same window should return the same instance")
	}

	// Getting a window for a timestamp in the same window should return the same instance
	timestamp2 := timestamp.Add(time.Minute)
	window3 := wm.GetWindow(timestamp2)
	if window != window3 {
		t.Error("Timestamps in the same window should return the same window instance")
	}

	// Getting a window for a different time window should return a different instance
	timestamp3 := timestamp.Add(10 * time.Minute)
	window4 := wm.GetWindow(timestamp3)
	if window == window4 {
		t.Error("Different time windows should return different window instances")
	}
}

func TestWindowManager_GetActiveWindows(t *testing.T) {
	windowSize := 5 * time.Minute
	wm := NewWindowManager(windowSize)

	// Initially no active windows
	activeWindows := wm.GetActiveWindows()
	if len(activeWindows) != 0 {
		t.Errorf("Expected 0 active windows, got %d", len(activeWindows))
	}

	// Create a few windows
	timestamp1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	timestamp2 := timestamp1.Add(10 * time.Minute)
	timestamp3 := timestamp1.Add(20 * time.Minute)

	wm.GetWindow(timestamp1)
	wm.GetWindow(timestamp2)
	wm.GetWindow(timestamp3)

	activeWindows = wm.GetActiveWindows()
	if len(activeWindows) != 3 {
		t.Errorf("Expected 3 active windows, got %d", len(activeWindows))
	}
}

func TestWindowManager_ExpireWindows(t *testing.T) {
	windowSize := 5 * time.Minute
	wm := NewWindowManager(windowSize)

	// Create windows at different times
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	timestamp1 := baseTime
	timestamp2 := baseTime.Add(10 * time.Minute)
	timestamp3 := baseTime.Add(20 * time.Minute)

	wm.GetWindow(timestamp1)
	wm.GetWindow(timestamp2)
	wm.GetWindow(timestamp3)

	// Verify 3 active windows
	activeWindows := wm.GetActiveWindows()
	if len(activeWindows) != 3 {
		t.Errorf("Expected 3 active windows, got %d", len(activeWindows))
	}

	// Expire windows older than 15 minutes from base time
	// Windows:
	// - Window 1: 12:00-12:05 (ends at 12:05, should expire)
	// - Window 2: 12:10-12:15 (ends at 12:15, should NOT expire - equals cutoff)
	// - Window 3: 12:20-12:25 (ends at 12:25, should NOT expire)
	cutoffTime := baseTime.Add(15 * time.Minute)
	expiredWindows, err := wm.ExpireWindows(cutoffTime)
	if err != nil {
		t.Fatalf("Failed to expire windows: %v", err)
	}

	// Should expire 1 window (only the first one ending before 12:15)
	if len(expiredWindows) != 1 {
		t.Errorf("Expected 1 expired window, got %d", len(expiredWindows))
	}

	// Should have 2 active windows remaining
	activeWindows = wm.GetActiveWindows()
	if len(activeWindows) != 2 {
		t.Errorf("Expected 2 active windows remaining, got %d", len(activeWindows))
	}
}

func TestWindowManager_StartStop(t *testing.T) {
	windowSize := 5 * time.Minute
	wm := NewWindowManager(windowSize)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the window manager
	err := wm.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start window manager: %v", err)
	}

	// Starting again should be safe
	err = wm.Start(ctx)
	if err != nil {
		t.Errorf("Starting already running window manager should not error: %v", err)
	}

	// Stop the window manager
	err = wm.Stop()
	if err != nil {
		t.Fatalf("Failed to stop window manager: %v", err)
	}

	// Stopping again should be safe
	err = wm.Stop()
	if err != nil {
		t.Errorf("Stopping already stopped window manager should not error: %v", err)
	}
}

func TestNewWindowedStream(t *testing.T) {
	// Create a base stream for testing
	storage := &storage.Storage{} // Mock storage
	config := &ProcessorConfig{
		ProcessorName:   "test-processor",
		ConsumerGroup:   "test-group",
		MaxConcurrency:  2,
		BatchSize:       10,
		FlushInterval:   time.Second,
		StateStoreType:  "memory",
	}
	
	processor, err := NewStreamProcessor(config, storage)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	baseStream := NewStream("test-topic", processor)
	windowSize := 5 * time.Minute

	windowedStream := NewWindowedStream(baseStream, windowSize)

	if windowedStream == nil {
		t.Fatal("NewWindowedStream returned nil")
	}

	if windowedStream.baseStream != baseStream {
		t.Error("Base stream not set correctly")
	}

	if windowedStream.windowSize != windowSize {
		t.Errorf("Expected window size %v, got %v", windowSize, windowedStream.windowSize)
	}

	if windowedStream.windowManager == nil {
		t.Error("Window manager not created")
	}

	if windowedStream.windowManager.GetWindowSize() != windowSize {
		t.Errorf("Expected window manager size %v, got %v", windowSize, windowedStream.windowManager.GetWindowSize())
	}
}

func TestWindowedStream_GroupBy(t *testing.T) {
	// Create a base stream for testing
	storage := &storage.Storage{} // Mock storage
	config := &ProcessorConfig{
		ProcessorName:   "test-processor",
		ConsumerGroup:   "test-group",
		MaxConcurrency:  2,
		BatchSize:       10,
		FlushInterval:   time.Second,
		StateStoreType:  "memory",
	}
	
	processor, err := NewStreamProcessor(config, storage)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	baseStream := NewStream("test-topic", processor)
	windowedStream := NewWindowedStream(baseStream, 5*time.Minute)

	// Create windowed grouped stream
	windowedGroupedStream := windowedStream.GroupBy(func(event *Event) string {
		return event.Key
	})

	if windowedGroupedStream == nil {
		t.Fatal("GroupBy returned nil")
	}

	// Verify it returns the correct type
	if _, ok := windowedGroupedStream.(*windowedGroupedStreamImpl); !ok {
		t.Error("GroupBy should return a windowedGroupedStreamImpl")
	}
}

func TestWindowedStream_AggregationOperations(t *testing.T) {
	// Create a base stream for testing
	storage := &storage.Storage{} // Mock storage
	config := &ProcessorConfig{
		ProcessorName:   "test-processor",
		ConsumerGroup:   "test-group",
		MaxConcurrency:  2,
		BatchSize:       10,
		FlushInterval:   time.Second,
		StateStoreType:  "memory",
	}
	
	processor, err := NewStreamProcessor(config, storage)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	baseStream := NewStream("test-topic", processor)
	windowedStream := NewWindowedStream(baseStream, 5*time.Minute)

	// Test Count
	countStream := windowedStream.Count()
	if countStream == nil {
		t.Error("Count returned nil")
	}
	if _, ok := countStream.(*aggregatedStreamImpl); !ok {
		t.Error("Count should return an aggregatedStreamImpl")
	}

	// Test Sum
	sumStream := windowedStream.Sum(ValueAsFloat64)
	if sumStream == nil {
		t.Error("Sum returned nil")
	}
	if _, ok := sumStream.(*aggregatedStreamImpl); !ok {
		t.Error("Sum should return an aggregatedStreamImpl")
	}

	// Test Average
	avgStream := windowedStream.Average(ValueAsFloat64)
	if avgStream == nil {
		t.Error("Average returned nil")
	}
	if _, ok := avgStream.(*aggregatedStreamImpl); !ok {
		t.Error("Average should return an aggregatedStreamImpl")
	}

	// Test Min
	minStream := windowedStream.Min(ValueAsFloat64)
	if minStream == nil {
		t.Error("Min returned nil")
	}
	if _, ok := minStream.(*aggregatedStreamImpl); !ok {
		t.Error("Min should return an aggregatedStreamImpl")
	}

	// Test Max
	maxStream := windowedStream.Max(ValueAsFloat64)
	if maxStream == nil {
		t.Error("Max returned nil")
	}
	if _, ok := maxStream.(*aggregatedStreamImpl); !ok {
		t.Error("Max should return an aggregatedStreamImpl")
	}

	// Test Reduce
	reduceStream := windowedStream.Reduce(func(events []*Event) *AggregateResult {
		return &AggregateResult{
			Count:     int64(len(events)),
			Timestamp: time.Now(),
		}
	})
	if reduceStream == nil {
		t.Error("Reduce returned nil")
	}
	if _, ok := reduceStream.(*aggregatedStreamImpl); !ok {
		t.Error("Reduce should return an aggregatedStreamImpl")
	}
}

func TestWindowedStream_GetMetrics(t *testing.T) {
	// Create a base stream for testing
	storage := &storage.Storage{} // Mock storage
	config := &ProcessorConfig{
		ProcessorName:   "test-processor",
		ConsumerGroup:   "test-group",
		MaxConcurrency:  2,
		BatchSize:       10,
		FlushInterval:   time.Second,
		StateStoreType:  "memory",
	}
	
	processor, err := NewStreamProcessor(config, storage)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	baseStream := NewStream("test-topic", processor)
	windowedStream := NewWindowedStream(baseStream, 5*time.Minute)

	metrics := windowedStream.GetMetrics()
	if metrics == nil {
		t.Fatal("GetMetrics returned nil")
	}

	// Verify initial state
	if metrics.EventsProcessed != 0 {
		t.Errorf("Expected EventsProcessed to be 0, got %d", metrics.EventsProcessed)
	}
	if metrics.WindowsCreated != 0 {
		t.Errorf("Expected WindowsCreated to be 0, got %d", metrics.WindowsCreated)
	}
	if metrics.WindowsExpired != 0 {
		t.Errorf("Expected WindowsExpired to be 0, got %d", metrics.WindowsExpired)
	}
}