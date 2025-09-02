package stream

import (
	"encoding/json"
	"time"
)

// Event represents a stream processing event with enhanced metadata
type Event struct {
	Key       string                 `json:"key"`
	Value     []byte                 `json:"value"`
	Headers   map[string]string      `json:"headers,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Partition int32                  `json:"partition"`
	Offset    int64                  `json:"offset"`
	Topic     string                 `json:"topic"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// GetValueAsString returns the event value as a string
func (e *Event) GetValueAsString() string {
	return string(e.Value)
}

// GetValueAsJSON unmarshals the event value as JSON into the provided interface
func (e *Event) GetValueAsJSON(v interface{}) error {
	return json.Unmarshal(e.Value, v)
}

// SetValueFromJSON marshals the provided interface as JSON and sets it as the event value
func (e *Event) SetValueFromJSON(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	e.Value = data
	return nil
}

// Clone creates a deep copy of the event
func (e *Event) Clone() *Event {
	clone := &Event{
		Key:       e.Key,
		Value:     make([]byte, len(e.Value)),
		Headers:   make(map[string]string),
		Timestamp: e.Timestamp,
		Partition: e.Partition,
		Offset:    e.Offset,
		Topic:     e.Topic,
		Metadata:  make(map[string]interface{}),
	}
	
	copy(clone.Value, e.Value)
	
	for k, v := range e.Headers {
		clone.Headers[k] = v
	}
	
	for k, v := range e.Metadata {
		clone.Metadata[k] = v
	}
	
	return clone
}

// Window represents a time window for stream processing
type Window struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// Contains checks if a timestamp falls within the window
func (w *Window) Contains(timestamp time.Time) bool {
	return !timestamp.Before(w.Start) && timestamp.Before(w.End)
}

// Duration returns the duration of the window
func (w *Window) Duration() time.Duration {
	return w.End.Sub(w.Start)
}

// String returns a string representation of the window
func (w *Window) String() string {
	return w.Start.Format(time.RFC3339) + " to " + w.End.Format(time.RFC3339)
}

// WindowedEvent represents an event with its associated window
type WindowedEvent struct {
	Event  *Event  `json:"event"`
	Window *Window `json:"window"`
}

// AggregateResult represents the result of an aggregation operation
type AggregateResult struct {
	Key       string                 `json:"key"`
	Window    *Window                `json:"window,omitempty"`
	Count     int64                  `json:"count,omitempty"`
	Sum       float64                `json:"sum,omitempty"`
	Average   float64                `json:"average,omitempty"`
	Min       float64                `json:"min,omitempty"`
	Max       float64                `json:"max,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

// ProcessorMetrics holds metrics for stream processing operations
type ProcessorMetrics struct {
	EventsProcessed  int64         `json:"events_processed"`
	EventsFiltered   int64         `json:"events_filtered"`
	EventsTransformed int64        `json:"events_transformed"`
	EventsAggregated int64         `json:"events_aggregated"`
	WindowsCreated   int64         `json:"windows_created"`
	WindowsExpired   int64         `json:"windows_expired"`
	ProcessingErrors int64         `json:"processing_errors"`
	AvgLatency       time.Duration `json:"avg_latency"`
	ThroughputPerSec float64       `json:"throughput_per_sec"`
}

// FilterFunc defines a function type for filtering events
type FilterFunc func(*Event) bool

// MapFunc defines a function type for transforming events
type MapFunc func(*Event) *Event

// KeyExtractorFunc defines a function type for extracting keys from events
type KeyExtractorFunc func(*Event) string

// ValueExtractorFunc defines a function type for extracting numeric values from events
type ValueExtractorFunc func(*Event) (float64, error)

// AggregatorFunc defines a function type for custom aggregations
type AggregatorFunc func([]*Event) *AggregateResult