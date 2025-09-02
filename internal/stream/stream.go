package stream

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/storage"
)

// streamImpl implements the Stream interface
type streamImpl struct {
	inputTopic   string
	processor    *StreamProcessorImpl
	operations   []Operation
	metrics      *ProcessorMetrics
	mu           sync.RWMutex
}

// Operation represents a stream processing operation
type Operation struct {
	Type   string      `json:"type"`
	Config interface{} `json:"config"`
}

// FilterOperation configuration
type FilterOperation struct {
	FilterFunc FilterFunc `json:"-"` // Not JSON serializable
}

// MapOperation configuration  
type MapOperation struct {
	MapFunc MapFunc `json:"-"` // Not JSON serializable
}

// NewStream creates a new stream processing pipeline
func NewStream(inputTopic string, processor *StreamProcessorImpl) *streamImpl {
	return &streamImpl{
		inputTopic:  inputTopic,
		processor:   processor,
		operations:  make([]Operation, 0),
		metrics:     &ProcessorMetrics{},
	}
}

// Filter adds a filter operation to the stream
func (s *streamImpl) Filter(filterFunc FilterFunc) Stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.operations = append(s.operations, Operation{
		Type:   "filter",
		Config: &FilterOperation{FilterFunc: filterFunc},
	})
	
	return s
}

// Map adds a map operation to the stream
func (s *streamImpl) Map(mapFunc MapFunc) Stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.operations = append(s.operations, Operation{
		Type:   "map",
		Config: &MapOperation{MapFunc: mapFunc},
	})
	
	return s
}

// Window creates a windowed stream with the specified duration
func (s *streamImpl) Window(duration time.Duration) WindowedStream {
	return NewWindowedStream(s, duration)
}

// SessionWindow creates a session windowed stream with the specified timeout
func (s *streamImpl) SessionWindow(timeout time.Duration) WindowedStream {
	// For now, implement as tumbling window - session windows can be enhanced later
	return NewWindowedStream(s, timeout)
}

// GroupBy creates a grouped stream
func (s *streamImpl) GroupBy(keyExtractor KeyExtractorFunc) GroupedStream {
	return NewGroupedStream(s, keyExtractor)
}

// ForEach processes each event with the provided function
func (s *streamImpl) ForEach(processor func(*Event)) error {
	// This is a terminal operation that starts processing
	return s.processor.processStream(s.inputTopic, func(event *Event) error {
		// Apply all operations in sequence
		processedEvent, shouldProcess := s.applyOperations(event)
		if shouldProcess && processedEvent != nil {
			processor(processedEvent)
			atomic.AddInt64(&s.metrics.EventsProcessed, 1)
		}
		return nil
	})
}

// Output sends processed events to the specified topic
func (s *streamImpl) Output(topic string) error {
	return s.processor.processStream(s.inputTopic, func(event *Event) error {
		// Apply all operations in sequence
		processedEvent, shouldProcess := s.applyOperations(event)
		if shouldProcess && processedEvent != nil {
			// Convert back to storage.Message and produce to output topic
			outputMsg := &storage.Message{
				Key:   processedEvent.Key,
				Value: processedEvent.Value,
			}
			
			_, _, err := s.processor.storage.Produce(topic, outputMsg)
			if err != nil {
				atomic.AddInt64(&s.metrics.ProcessingErrors, 1)
				log.Printf("[Stream] Failed to produce to output topic %s: %v", topic, err)
				return err
			}
			
			atomic.AddInt64(&s.metrics.EventsProcessed, 1)
		}
		return nil
	})
}

// Start begins processing the stream
func (s *streamImpl) Start(ctx context.Context) error {
	return s.processor.Start(ctx)
}

// Stop stops stream processing
func (s *streamImpl) Stop() error {
	return s.processor.Stop()
}

// GetMetrics returns the current processing metrics
func (s *streamImpl) GetMetrics() *ProcessorMetrics {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// Return a copy of metrics
	return &ProcessorMetrics{
		EventsProcessed:   atomic.LoadInt64(&s.metrics.EventsProcessed),
		EventsFiltered:    atomic.LoadInt64(&s.metrics.EventsFiltered),
		EventsTransformed: atomic.LoadInt64(&s.metrics.EventsTransformed),
		ProcessingErrors:  atomic.LoadInt64(&s.metrics.ProcessingErrors),
		AvgLatency:        s.metrics.AvgLatency,
		ThroughputPerSec:  s.metrics.ThroughputPerSec,
	}
}

// applyOperations applies all operations to an event in sequence
func (s *streamImpl) applyOperations(event *Event) (*Event, bool) {
	currentEvent := event
	
	for _, operation := range s.operations {
		switch operation.Type {
		case "filter":
			config := operation.Config.(*FilterOperation)
			if !config.FilterFunc(currentEvent) {
				atomic.AddInt64(&s.metrics.EventsFiltered, 1)
				return nil, false
			}
			
		case "map":
			config := operation.Config.(*MapOperation)
			currentEvent = config.MapFunc(currentEvent)
			if currentEvent == nil {
				return nil, false
			}
			atomic.AddInt64(&s.metrics.EventsTransformed, 1)
		}
	}
	
	return currentEvent, true
}

// convertStorageMessageToEvent converts a storage.Message to a stream.Event
func convertStorageMessageToEvent(msg *storage.Message, topic string) *Event {
	return &Event{
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   make(map[string]string),
		Timestamp: msg.Timestamp,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Topic:     topic,
		Metadata:  make(map[string]interface{}),
	}
}

// Helper functions for common operations

// IdentityMap is a map function that returns the event unchanged
var IdentityMap = func(event *Event) *Event {
	return event
}

// TrueFilter is a filter that accepts all events
var TrueFilter = func(event *Event) bool {
	return true
}

// KeyByValue extracts the key from the event's key field
var KeyByValue = func(event *Event) string {
	return event.Key
}

// KeyByTopic extracts the key from the event's topic
var KeyByTopic = func(event *Event) string {
	return event.Topic
}

// ValueAsFloat64 extracts a float64 value from the event's value (expects JSON number)
var ValueAsFloat64 = func(event *Event) (float64, error) {
	var value float64
	err := event.GetValueAsJSON(&value)
	return value, err
}

// Common filter functions

// FilterByTopic creates a filter that only accepts events from specified topics
func FilterByTopic(topics ...string) FilterFunc {
	topicSet := make(map[string]bool)
	for _, topic := range topics {
		topicSet[topic] = true
	}
	
	return func(event *Event) bool {
		return topicSet[event.Topic]
	}
}

// FilterByKeyPrefix creates a filter that only accepts events with keys starting with prefix
func FilterByKeyPrefix(prefix string) FilterFunc {
	return func(event *Event) bool {
		return len(event.Key) >= len(prefix) && event.Key[:len(prefix)] == prefix
	}
}

// FilterByTimeRange creates a filter that only accepts events within a time range
func FilterByTimeRange(start, end time.Time) FilterFunc {
	return func(event *Event) bool {
		return !event.Timestamp.Before(start) && !event.Timestamp.After(end)
	}
}

// Common map functions

// MapAddTimestamp adds a timestamp field to the event's metadata
func MapAddTimestamp(event *Event) *Event {
	clone := event.Clone()
	if clone.Metadata == nil {
		clone.Metadata = make(map[string]interface{})
	}
	clone.Metadata["processed_at"] = time.Now()
	return clone
}

// MapAddHeader adds a header to the event
func MapAddHeader(key, value string) MapFunc {
	return func(event *Event) *Event {
		clone := event.Clone()
		if clone.Headers == nil {
			clone.Headers = make(map[string]string)
		}
		clone.Headers[key] = value
		return clone
	}
}

// MapTransformKey transforms the event key using the provided function
func MapTransformKey(transformer func(string) string) MapFunc {
	return func(event *Event) *Event {
		clone := event.Clone()
		clone.Key = transformer(clone.Key)
		return clone
	}
}

// MapTransformValue transforms the event value as a string
func MapTransformValue(transformer func(string) string) MapFunc {
	return func(event *Event) *Event {
		clone := event.Clone()
		transformedValue := transformer(string(clone.Value))
		clone.Value = []byte(transformedValue)
		return clone
	}
}