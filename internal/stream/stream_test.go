package stream

import (
	"testing"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/storage"
)

func TestNewStream(t *testing.T) {
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

	stream := NewStream("test-topic", processor)
	
	if stream == nil {
		t.Fatal("NewStream returned nil")
	}
	if stream.inputTopic != "test-topic" {
		t.Errorf("Expected input topic 'test-topic', got '%s'", stream.inputTopic)
	}
	if stream.processor != processor {
		t.Error("Processor not set correctly")
	}
	if len(stream.operations) != 0 {
		t.Errorf("Expected 0 operations, got %d", len(stream.operations))
	}
}

func TestStream_Filter(t *testing.T) {
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

	stream := NewStream("test-topic", processor)
	
	// Add filter operation
	filteredStream := stream.Filter(func(event *Event) bool {
		return event.Key == "keep"
	})

	// Should return the same stream instance (fluent interface)
	if filteredStream != stream {
		t.Error("Filter should return the same stream instance")
	}

	// Check that operation was added
	if len(stream.operations) != 1 {
		t.Errorf("Expected 1 operation, got %d", len(stream.operations))
	}

	if stream.operations[0].Type != "filter" {
		t.Errorf("Expected operation type 'filter', got '%s'", stream.operations[0].Type)
	}
}

func TestStream_Map(t *testing.T) {
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

	stream := NewStream("test-topic", processor)
	
	// Add map operation
	mappedStream := stream.Map(func(event *Event) *Event {
		clone := event.Clone()
		clone.Key = "mapped-" + clone.Key
		return clone
	})

	// Should return the same stream instance (fluent interface)
	if mappedStream != stream {
		t.Error("Map should return the same stream instance")
	}

	// Check that operation was added
	if len(stream.operations) != 1 {
		t.Errorf("Expected 1 operation, got %d", len(stream.operations))
	}

	if stream.operations[0].Type != "map" {
		t.Errorf("Expected operation type 'map', got '%s'", stream.operations[0].Type)
	}
}

func TestStream_Window(t *testing.T) {
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

	stream := NewStream("test-topic", processor)
	
	// Create windowed stream
	windowedStream := stream.Window(time.Minute)

	if windowedStream == nil {
		t.Fatal("Window returned nil")
	}

	// Verify it returns a different type (WindowedStream)
	if _, ok := windowedStream.(*windowedStreamImpl); !ok {
		t.Error("Window should return a windowedStreamImpl")
	}
}

func TestStream_GroupBy(t *testing.T) {
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

	stream := NewStream("test-topic", processor)
	
	// Create grouped stream
	groupedStream := stream.GroupBy(func(event *Event) string {
		return event.Key
	})

	if groupedStream == nil {
		t.Fatal("GroupBy returned nil")
	}

	// Verify it returns a different type (GroupedStream)
	if _, ok := groupedStream.(*groupedStreamImpl); !ok {
		t.Error("GroupBy should return a groupedStreamImpl")
	}
}

func TestStream_ApplyOperations(t *testing.T) {
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

	stream := NewStream("test-topic", processor)
	
	// Create a test event
	event := &Event{
		Key:     "test-key",
		Value:   []byte("test-value"),
		Headers: make(map[string]string),
	}

	// Test with no operations
	result, shouldProcess := stream.applyOperations(event)
	if !shouldProcess {
		t.Error("Should process event with no operations")
	}
	if result != event {
		t.Error("Should return same event with no operations")
	}

	// Test with filter that passes
	stream.Filter(func(e *Event) bool {
		return e.Key == "test-key"
	})
	
	result, shouldProcess = stream.applyOperations(event)
	if !shouldProcess {
		t.Error("Should process event that passes filter")
	}

	// Test with filter that fails
	stream = NewStream("test-topic", processor)
	stream.Filter(func(e *Event) bool {
		return e.Key == "wrong-key"
	})
	
	result, shouldProcess = stream.applyOperations(event)
	if shouldProcess {
		t.Error("Should not process event that fails filter")
	}
	if result != nil {
		t.Error("Should return nil for filtered out event")
	}

	// Test with map operation
	stream = NewStream("test-topic", processor)
	stream.Map(func(e *Event) *Event {
		clone := e.Clone()
		clone.Key = "mapped-" + clone.Key
		return clone
	})
	
	result, shouldProcess = stream.applyOperations(event)
	if !shouldProcess {
		t.Error("Should process event through map operation")
	}
	if result.Key != "mapped-test-key" {
		t.Errorf("Expected mapped key 'mapped-test-key', got '%s'", result.Key)
	}

	// Test with chained operations (filter then map)
	stream = NewStream("test-topic", processor)
	stream.Filter(func(e *Event) bool {
		return e.Key == "test-key"
	}).Map(func(e *Event) *Event {
		clone := e.Clone()
		clone.Key = "chained-" + clone.Key
		return clone
	})
	
	result, shouldProcess = stream.applyOperations(event)
	if !shouldProcess {
		t.Error("Should process event through chained operations")
	}
	if result.Key != "chained-test-key" {
		t.Errorf("Expected chained key 'chained-test-key', got '%s'", result.Key)
	}
}

func TestStream_GetMetrics(t *testing.T) {
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

	stream := NewStream("test-topic", processor)
	
	metrics := stream.GetMetrics()
	if metrics == nil {
		t.Fatal("GetMetrics returned nil")
	}

	// Verify initial state
	if metrics.EventsProcessed != 0 {
		t.Errorf("Expected EventsProcessed to be 0, got %d", metrics.EventsProcessed)
	}
	if metrics.EventsFiltered != 0 {
		t.Errorf("Expected EventsFiltered to be 0, got %d", metrics.EventsFiltered)
	}
	if metrics.ProcessingErrors != 0 {
		t.Errorf("Expected ProcessingErrors to be 0, got %d", metrics.ProcessingErrors)
	}
}

func TestStreamProcessorConfig_Defaults(t *testing.T) {
	storage := &storage.Storage{} // Mock storage
	
	// Test with minimal config
	config := &ProcessorConfig{}
	
	processor, err := NewStreamProcessor(config, storage)
	if err != nil {
		t.Fatalf("Failed to create processor with minimal config: %v", err)
	}

	// Check defaults were applied
	if processor.config.MaxConcurrency != 4 {
		t.Errorf("Expected MaxConcurrency default 4, got %d", processor.config.MaxConcurrency)
	}
	if processor.config.BatchSize != 100 {
		t.Errorf("Expected BatchSize default 100, got %d", processor.config.BatchSize)
	}
	if processor.config.FlushInterval != time.Second {
		t.Errorf("Expected FlushInterval default 1s, got %v", processor.config.FlushInterval)
	}
	if processor.config.ProcessorName == "" {
		t.Error("ProcessorName should not be empty after applying defaults")
	}
	if processor.config.ConsumerGroup == "" {
		t.Error("ConsumerGroup should not be empty after applying defaults")
	}
}

func TestStreamProcessor_IsRunning(t *testing.T) {
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

	// Should not be running initially
	if processor.IsRunning() {
		t.Error("Processor should not be running initially")
	}

	// Note: We can't easily test Start/Stop without a full integration test
	// because it requires actual gRPC connections
}

func TestStreamProcessor_GetConfig(t *testing.T) {
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

	retrievedConfig := processor.GetConfig()
	if retrievedConfig == nil {
		t.Fatal("GetConfig returned nil")
	}

	if retrievedConfig.ProcessorName != config.ProcessorName {
		t.Errorf("Expected ProcessorName %s, got %s", config.ProcessorName, retrievedConfig.ProcessorName)
	}
	if retrievedConfig.ConsumerGroup != config.ConsumerGroup {
		t.Errorf("Expected ConsumerGroup %s, got %s", config.ConsumerGroup, retrievedConfig.ConsumerGroup)
	}
}

func TestStreamProcessor_GetHealthStatus(t *testing.T) {
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

	status := processor.GetHealthStatus()
	if status != "stopped" {
		t.Errorf("Expected health status 'stopped', got '%s'", status)
	}
}