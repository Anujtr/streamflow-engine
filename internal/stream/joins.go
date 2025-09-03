package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// JoinType represents different types of stream joins
type JoinType string

const (
	InnerJoin JoinType = "inner"
	LeftJoin  JoinType = "left"
	RightJoin JoinType = "right"
	OuterJoin JoinType = "outer"
)

// JoinConfig holds configuration for stream joins
type JoinConfig struct {
	Type           JoinType          `json:"type"`
	WindowSize     time.Duration     `json:"window_size"`
	JoinFunc       JoinFunc          `json:"-"`
	LeftKeyFunc    KeyExtractorFunc  `json:"-"`
	RightKeyFunc   KeyExtractorFunc  `json:"-"`
	MaxBufferSize  int              `json:"max_buffer_size"`
}

// JoinBuffer holds events for joining
type JoinBuffer struct {
	events    map[string][]*Event // keyed by join key
	windows   map[string]*Window  // window per key
	mu        sync.RWMutex
	maxSize   int
}

// NewJoinBuffer creates a new join buffer
func NewJoinBuffer(maxSize int) *JoinBuffer {
	return &JoinBuffer{
		events:  make(map[string][]*Event),
		windows: make(map[string]*Window),
		maxSize: maxSize,
	}
}

// AddEvent adds an event to the join buffer
func (jb *JoinBuffer) AddEvent(key string, event *Event, window *Window) error {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	
	// Check buffer size limit
	if jb.getTotalEvents() >= jb.maxSize {
		return fmt.Errorf("join buffer full")
	}
	
	jb.events[key] = append(jb.events[key], event)
	jb.windows[key] = window
	
	return nil
}

// GetEvents returns events for a key within the window
func (jb *JoinBuffer) GetEvents(key string, window *Window) []*Event {
	jb.mu.RLock()
	defer jb.mu.RUnlock()
	
	events := jb.events[key]
	var matchingEvents []*Event
	
	for _, event := range events {
		if window.Contains(event.Timestamp) {
			matchingEvents = append(matchingEvents, event)
		}
	}
	
	return matchingEvents
}

// CleanupExpired removes events outside the window
func (jb *JoinBuffer) CleanupExpired(cutoffTime time.Time) {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	
	for key, events := range jb.events {
		var activeEvents []*Event
		for _, event := range events {
			if event.Timestamp.After(cutoffTime) {
				activeEvents = append(activeEvents, event)
			}
		}
		
		if len(activeEvents) == 0 {
			delete(jb.events, key)
			delete(jb.windows, key)
		} else {
			jb.events[key] = activeEvents
		}
	}
}

// getTotalEvents returns total number of buffered events
func (jb *JoinBuffer) getTotalEvents() int {
	count := 0
	for _, events := range jb.events {
		count += len(events)
	}
	return count
}

// StreamJoiner handles joining of two streams
type StreamJoiner struct {
	config      *JoinConfig
	leftBuffer  *JoinBuffer
	rightBuffer *JoinBuffer
	results     chan *JoinResult
	mu          sync.RWMutex
	running     bool
	stopCh      chan struct{}
}

// NewStreamJoiner creates a new stream joiner
func NewStreamJoiner(config *JoinConfig) *StreamJoiner {
	if config.MaxBufferSize == 0 {
		config.MaxBufferSize = 10000 // default buffer size
	}
	
	return &StreamJoiner{
		config:      config,
		leftBuffer:  NewJoinBuffer(config.MaxBufferSize / 2),
		rightBuffer: NewJoinBuffer(config.MaxBufferSize / 2),
		results:     make(chan *JoinResult, 1000),
		stopCh:      make(chan struct{}),
	}
}

// ProcessLeftEvent processes an event from the left stream
func (sj *StreamJoiner) ProcessLeftEvent(event *Event) error {
	if !sj.running {
		return fmt.Errorf("stream joiner not running")
	}
	
	leftKey := sj.config.LeftKeyFunc(event)
	window := &Window{
		Start: event.Timestamp.Add(-sj.config.WindowSize),
		End:   event.Timestamp.Add(time.Nanosecond), // Make end inclusive by adding 1 nanosecond
	}
	
	// Add to left buffer
	if err := sj.leftBuffer.AddEvent(leftKey, event, window); err != nil {
		return err
	}
	
	// Look for matches in right buffer
	rightEvents := sj.rightBuffer.GetEvents(leftKey, window)
	
	// Create join results
	if len(rightEvents) > 0 {
		// Inner/Left join - we have matches
		for _, rightEvent := range rightEvents {
			result := &JoinResult{
				LeftEvent:  event,
				RightEvent: rightEvent,
				JoinKey:    leftKey,
				Window:     window,
				Timestamp:  time.Now(),
			}
			select {
			case sj.results <- result:
			default:
				// Results buffer full, drop oldest
			}
		}
	} else if sj.config.Type == LeftJoin || sj.config.Type == OuterJoin {
		// Left/Outer join with no match
		result := &JoinResult{
			LeftEvent:  event,
			RightEvent: nil,
			JoinKey:    leftKey,
			Window:     window,
			Timestamp:  time.Now(),
		}
		select {
		case sj.results <- result:
		default:
			// Results buffer full, drop oldest
		}
	}
	
	return nil
}

// ProcessRightEvent processes an event from the right stream
func (sj *StreamJoiner) ProcessRightEvent(event *Event) error {
	if !sj.running {
		return fmt.Errorf("stream joiner not running")
	}
	
	rightKey := sj.config.RightKeyFunc(event)
	window := &Window{
		Start: event.Timestamp.Add(-sj.config.WindowSize),
		End:   event.Timestamp.Add(time.Nanosecond), // Make end inclusive by adding 1 nanosecond
	}
	
	// Add to right buffer
	if err := sj.rightBuffer.AddEvent(rightKey, event, window); err != nil {
		return err
	}
	
	// Look for matches in left buffer
	leftEvents := sj.leftBuffer.GetEvents(rightKey, window)
	
	// Create join results
	if len(leftEvents) > 0 {
		// Inner/Right join - we have matches
		for _, leftEvent := range leftEvents {
			// Only create result if we haven't already created it
			// (avoid duplicates from ProcessLeftEvent)
			if sj.config.Type == InnerJoin && leftEvent.Timestamp.After(event.Timestamp) {
				result := &JoinResult{
					LeftEvent:  leftEvent,
					RightEvent: event,
					JoinKey:    rightKey,
					Window:     window,
					Timestamp:  time.Now(),
				}
				select {
				case sj.results <- result:
				default:
					// Results buffer full, drop oldest
				}
			}
		}
	} else if sj.config.Type == RightJoin || sj.config.Type == OuterJoin {
		// Right/Outer join with no match
		result := &JoinResult{
			LeftEvent:  nil,
			RightEvent: event,
			JoinKey:    rightKey,
			Window:     window,
			Timestamp:  time.Now(),
		}
		select {
		case sj.results <- result:
		default:
			// Results buffer full, drop oldest
		}
	}
	
	return nil
}

// GetResults returns the channel for join results
func (sj *StreamJoiner) GetResults() <-chan *JoinResult {
	return sj.results
}

// Start starts the stream joiner
func (sj *StreamJoiner) Start(ctx context.Context) error {
	sj.mu.Lock()
	defer sj.mu.Unlock()
	
	if sj.running {
		return nil
	}
	
	sj.running = true
	sj.stopCh = make(chan struct{})
	
	// Start cleanup goroutine
	go sj.cleanupLoop(ctx)
	
	return nil
}

// Stop stops the stream joiner
func (sj *StreamJoiner) Stop() error {
	sj.mu.Lock()
	defer sj.mu.Unlock()
	
	if !sj.running {
		return nil
	}
	
	sj.running = false
	close(sj.stopCh)
	close(sj.results)
	
	return nil
}

// cleanupLoop periodically cleans up expired events
func (sj *StreamJoiner) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(sj.config.WindowSize / 2)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-sj.stopCh:
			return
		case <-ticker.C:
			cutoffTime := time.Now().Add(-sj.config.WindowSize)
			sj.leftBuffer.CleanupExpired(cutoffTime)
			sj.rightBuffer.CleanupExpired(cutoffTime)
		}
	}
}

// joinedStreamImpl implements JoinedStream interface
type joinedStreamImpl struct {
	joiner     *StreamJoiner
	leftStream  Stream
	rightStream Stream
	operations []Operation
	processor  StreamProcessor
}

// NewJoinedStream creates a new joined stream
func NewJoinedStream(leftStream, rightStream Stream, config *JoinConfig, processor StreamProcessor) JoinedStream {
	joiner := NewStreamJoiner(config)
	
	return &joinedStreamImpl{
		joiner:      joiner,
		leftStream:  leftStream,
		rightStream: rightStream,
		processor:   processor,
	}
}

// Filter applies a filter to join results
func (js *joinedStreamImpl) Filter(filterFunc func(*JoinResult) bool) JoinedStream {
	js.operations = append(js.operations, Operation{
		Type: "join_filter",
		Config: filterFunc,
	})
	return js
}

// Map transforms join results back to events
func (js *joinedStreamImpl) Map(mapFunc func(*JoinResult) *Event) Stream {
	// Start a goroutine to process join results and transform them
	go func() {
		for result := range js.joiner.GetResults() {
			event := mapFunc(result)
			if event != nil {
				// Process the transformed event through the processor
				// This would need integration with the processor
			}
		}
	}()
	
	// Return the left stream as the base (simplified implementation)
	return js.leftStream
}

// Window applies windowing to joined results
func (js *joinedStreamImpl) Window(duration time.Duration) WindowedStream {
	// Convert join results to events and apply windowing
	return js.leftStream.Window(duration)
}

// ForEach processes each join result
func (js *joinedStreamImpl) ForEach(forEach func(*JoinResult)) error {
	go func() {
		for result := range js.joiner.GetResults() {
			forEach(result)
		}
	}()
	return nil
}

// Output sends join results to a topic
func (js *joinedStreamImpl) Output(topic string) error {
	return js.ForEach(func(result *JoinResult) {
		// Transform to event and send to topic
		// This would need integration with the producer
	})
}

// Start starts the joined stream processing
func (js *joinedStreamImpl) Start(ctx context.Context) error {
	return js.joiner.Start(ctx)
}

// Stop stops the joined stream processing
func (js *joinedStreamImpl) Stop() error {
	return js.joiner.Stop()
}

// GetMetrics returns processing metrics
func (js *joinedStreamImpl) GetMetrics() *ProcessorMetrics {
	return js.processor.GetMetrics()
}

// StreamEnricher handles stream enrichment with external data sources
type StreamEnricher struct {
	source       EnrichmentSource
	keyExtractor KeyExtractorFunc
	mu           sync.RWMutex
}

// NewStreamEnricher creates a new stream enricher
func NewStreamEnricher(source EnrichmentSource, keyExtractor KeyExtractorFunc) *StreamEnricher {
	return &StreamEnricher{
		source:       source,
		keyExtractor: keyExtractor,
	}
}

// Enrich enriches an event with external data
func (se *StreamEnricher) Enrich(event *Event) (*Event, error) {
	se.mu.RLock()
	defer se.mu.RUnlock()
	
	key := se.keyExtractor(event)
	enrichmentData, err := se.source.Lookup(key)
	if err != nil {
		return event, err // Return original event if enrichment fails
	}
	
	// Clone the event and add enrichment data
	enrichedEvent := event.Clone()
	if enrichedEvent.Metadata == nil {
		enrichedEvent.Metadata = make(map[string]interface{})
	}
	
	// Parse enrichment data as JSON and add to metadata
	var enrichmentMap map[string]interface{}
	if err := json.Unmarshal(enrichmentData, &enrichmentMap); err == nil {
		enrichedEvent.Metadata["enrichment"] = enrichmentMap
	} else {
		// If not JSON, store as raw string
		enrichedEvent.Metadata["enrichment"] = string(enrichmentData)
	}
	
	return enrichedEvent, nil
}

// Close closes the enrichment source
func (se *StreamEnricher) Close() error {
	return se.source.Close()
}