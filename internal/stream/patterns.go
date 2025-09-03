package stream

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Pattern represents a complex event pattern
type Pattern struct {
	Name         string                `json:"name"`
	Matcher      PatternMatcherFunc    `json:"-"`
	WithinTime   time.Duration         `json:"within_time"`
	MaxEvents    int                   `json:"max_events"`
	MinEvents    int                   `json:"min_events"`
	KeyExtractor KeyExtractorFunc      `json:"-"`
}

// PatternState tracks the state of pattern matching for a key
type PatternState struct {
	PatternName   string    `json:"pattern_name"`
	Key           string    `json:"key"`
	Events        []*Event  `json:"events"`
	StartTime     time.Time `json:"start_time"`
	LastEventTime time.Time `json:"last_event_time"`
	Completed     bool      `json:"completed"`
}

// IsExpired checks if the pattern state has expired
func (ps *PatternState) IsExpired(withinTime time.Duration, currentTime time.Time) bool {
	return currentTime.Sub(ps.StartTime) > withinTime
}

// AddEvent adds an event to the pattern state
func (ps *PatternState) AddEvent(event *Event) {
	ps.Events = append(ps.Events, event)
	ps.LastEventTime = event.Timestamp
	if ps.StartTime.IsZero() {
		ps.StartTime = event.Timestamp
	}
}

// patternDetectorImpl implements PatternDetector interface
type patternDetectorImpl struct {
	patterns     map[string]*Pattern                       // pattern definitions
	patternState map[string]map[string]*PatternState       // [pattern_name][key] -> state
	results      chan *PatternResult                        // detected patterns
	mu           sync.RWMutex
	running      bool
	stopCh       chan struct{}
}

// NewPatternDetector creates a new pattern detector
func NewPatternDetector() PatternDetector {
	return &patternDetectorImpl{
		patterns:     make(map[string]*Pattern),
		patternState: make(map[string]map[string]*PatternState),
		results:      make(chan *PatternResult, 1000),
		stopCh:       make(chan struct{}),
	}
}

// AddPattern adds a pattern to detect
func (pd *patternDetectorImpl) AddPattern(name string, matcher PatternMatcherFunc, withinTime time.Duration) error {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	
	if _, exists := pd.patterns[name]; exists {
		return fmt.Errorf("pattern %s already exists", name)
	}
	
	pattern := &Pattern{
		Name:         name,
		Matcher:      matcher,
		WithinTime:   withinTime,
		MaxEvents:    100,  // default max events
		MinEvents:    2,    // default min events
		KeyExtractor: func(e *Event) string { return e.Key }, // default key extractor
	}
	
	pd.patterns[name] = pattern
	pd.patternState[name] = make(map[string]*PatternState)
	
	return nil
}

// RemovePattern removes a pattern
func (pd *patternDetectorImpl) RemovePattern(name string) error {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	
	if _, exists := pd.patterns[name]; !exists {
		return fmt.Errorf("pattern %s not found", name)
	}
	
	delete(pd.patterns, name)
	delete(pd.patternState, name)
	
	return nil
}

// ProcessEvent processes an event against all patterns
func (pd *patternDetectorImpl) ProcessEvent(event *Event) ([]*PatternResult, error) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	
	var results []*PatternResult
	
	for patternName, pattern := range pd.patterns {
		key := pattern.KeyExtractor(event)
		
		// Get or create pattern state
		if pd.patternState[patternName] == nil {
			pd.patternState[patternName] = make(map[string]*PatternState)
		}
		
		state, exists := pd.patternState[patternName][key]
		if !exists {
			state = &PatternState{
				PatternName: patternName,
				Key:         key,
				Events:      make([]*Event, 0),
			}
			pd.patternState[patternName][key] = state
		}
		
		// Check if existing state is expired
		if state.IsExpired(pattern.WithinTime, event.Timestamp) {
			// Reset expired state
			state = &PatternState{
				PatternName: patternName,
				Key:         key,
				Events:      make([]*Event, 0),
			}
			pd.patternState[patternName][key] = state
		}
		
		// Add event to state
		state.AddEvent(event)
		
		// Check if pattern is complete
		if len(state.Events) >= pattern.MinEvents {
			if pattern.Matcher(state.Events) {
				// Pattern matched!
				result := &PatternResult{
					PatternName: patternName,
					Events:      make([]*Event, len(state.Events)),
					StartTime:   state.StartTime,
					EndTime:     event.Timestamp,
					Metadata:    make(map[string]interface{}),
				}
				
				// Copy events
				copy(result.Events, state.Events)
				
				// Add metadata
				result.Metadata["key"] = key
				result.Metadata["duration"] = result.EndTime.Sub(result.StartTime)
				result.Metadata["event_count"] = len(result.Events)
				
				results = append(results, result)
				
				// Send to results channel
				select {
				case pd.results <- result:
				default:
					// Channel full, drop result
				}
				
				// Mark as completed and reset state
				state.Completed = true
				pd.patternState[patternName][key] = &PatternState{
					PatternName: patternName,
					Key:         key,
					Events:      make([]*Event, 0),
				}
			}
		}
		
		// Clean up if we've exceeded max events without matching
		if len(state.Events) >= pattern.MaxEvents {
			// Reset state
			pd.patternState[patternName][key] = &PatternState{
				PatternName: patternName,
				Key:         key,
				Events:      make([]*Event, 0),
			}
		}
	}
	
	return results, nil
}

// GetPatternState returns the current state for a pattern
func (pd *patternDetectorImpl) GetPatternState(patternName string) (map[string]interface{}, error) {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	
	if _, exists := pd.patterns[patternName]; !exists {
		return nil, fmt.Errorf("pattern %s not found", patternName)
	}
	
	state := make(map[string]interface{})
	states := pd.patternState[patternName]
	
	state["active_keys"] = len(states)
	state["total_events"] = 0
	
	for _, patternState := range states {
		state["total_events"] = state["total_events"].(int) + len(patternState.Events)
	}
	
	return state, nil
}

// ClearPatternState clears state for a pattern
func (pd *patternDetectorImpl) ClearPatternState(patternName string) error {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	
	if _, exists := pd.patterns[patternName]; !exists {
		return fmt.Errorf("pattern %s not found", patternName)
	}
	
	pd.patternState[patternName] = make(map[string]*PatternState)
	
	return nil
}

// Start starts the pattern detector
func (pd *patternDetectorImpl) Start(ctx context.Context) error {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	
	if pd.running {
		return nil
	}
	
	pd.running = true
	pd.stopCh = make(chan struct{})
	
	// Start cleanup goroutine
	go pd.cleanupLoop(ctx)
	
	return nil
}

// Stop stops the pattern detector
func (pd *patternDetectorImpl) Stop() error {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	
	if !pd.running {
		return nil
	}
	
	pd.running = false
	close(pd.stopCh)
	close(pd.results)
	
	return nil
}

// cleanupLoop periodically cleans up expired pattern states
func (pd *patternDetectorImpl) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // cleanup every 30 seconds
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-pd.stopCh:
			return
		case <-ticker.C:
			pd.cleanupExpiredStates()
		}
	}
}

// cleanupExpiredStates removes expired pattern states
func (pd *patternDetectorImpl) cleanupExpiredStates() {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	
	currentTime := time.Now()
	
	for patternName, pattern := range pd.patterns {
		states := pd.patternState[patternName]
		
		for key, state := range states {
			if state.IsExpired(pattern.WithinTime, currentTime) {
				delete(states, key)
			}
		}
	}
}

// GetResults returns the channel for pattern results
func (pd *patternDetectorImpl) GetResults() <-chan *PatternResult {
	return pd.results
}

// patternStreamImpl implements PatternStream interface
type patternStreamImpl struct {
	detector   PatternDetector
	baseStream Stream
	operations []Operation
	processor  StreamProcessor
}

// NewPatternStream creates a new pattern stream
func NewPatternStream(baseStream Stream, detector PatternDetector, processor StreamProcessor) PatternStream {
	return &patternStreamImpl{
		detector:   detector,
		baseStream: baseStream,
		processor:  processor,
	}
}

// Filter applies a filter to pattern results
func (ps *patternStreamImpl) Filter(filterFunc func(*PatternResult) bool) PatternStream {
	ps.operations = append(ps.operations, Operation{
		Type:   "pattern_filter",
		Config: filterFunc,
	})
	return ps
}

// Map transforms pattern results back to events
func (ps *patternStreamImpl) Map(mapFunc func(*PatternResult) *Event) Stream {
	// Start a goroutine to process pattern results and transform them
	go func() {
		for result := range ps.detector.(*patternDetectorImpl).results {
			event := mapFunc(result)
			if event != nil {
				// Process the transformed event
				// This would need integration with the processor
			}
		}
	}()
	
	// Return the base stream (simplified implementation)
	return ps.baseStream
}

// ForEach processes each pattern result
func (ps *patternStreamImpl) ForEach(forEach func(*PatternResult)) error {
	go func() {
		for result := range ps.detector.(*patternDetectorImpl).results {
			forEach(result)
		}
	}()
	return nil
}

// Output sends pattern results to a topic
func (ps *patternStreamImpl) Output(topic string) error {
	return ps.ForEach(func(result *PatternResult) {
		// Transform to event and send to topic
		// This would need integration with the producer
	})
}

// Alert sends pattern results to an alert topic
func (ps *patternStreamImpl) Alert(alertTopic string) error {
	return ps.ForEach(func(result *PatternResult) {
		// Send alert with pattern result details
		// This would typically create an alert event with:
		// - Pattern name
		// - Matched events
		// - Timestamp range
		// - Severity level
	})
}

// Start starts the pattern stream processing
func (ps *patternStreamImpl) Start(ctx context.Context) error {
	return ps.detector.Start(ctx)
}

// Stop stops the pattern stream processing
func (ps *patternStreamImpl) Stop() error {
	return ps.detector.Stop()
}

// GetMetrics returns processing metrics
func (ps *patternStreamImpl) GetMetrics() *ProcessorMetrics {
	return ps.processor.GetMetrics()
}

// Common pattern matchers

// SequencePattern creates a pattern matcher for event sequences
func SequencePattern(eventTypes ...string) PatternMatcherFunc {
	return func(events []*Event) bool {
		if len(events) < len(eventTypes) {
			return false
		}
		
		// Check if events match the sequence
		for i, expectedType := range eventTypes {
			if i >= len(events) {
				return false
			}
			
			// Extract event type from metadata or key
			eventType := events[i].Key
			if events[i].Metadata != nil {
				if et, exists := events[i].Metadata["event_type"]; exists {
					eventType = fmt.Sprintf("%v", et)
				}
			}
			
			if eventType != expectedType {
				return false
			}
		}
		
		return true
	}
}

// ThresholdPattern creates a pattern matcher for threshold violations
func ThresholdPattern(valueExtractor ValueExtractorFunc, threshold float64, operator string) PatternMatcherFunc {
	return func(events []*Event) bool {
		for _, event := range events {
			value, err := valueExtractor(event)
			if err != nil {
				continue
			}
			
			switch operator {
			case ">":
				if value > threshold {
					return true
				}
			case "<":
				if value < threshold {
					return true
				}
			case ">=":
				if value >= threshold {
					return true
				}
			case "<=":
				if value <= threshold {
					return true
				}
			case "==":
				if value == threshold {
					return true
				}
			}
		}
		
		return false
	}
}

// FrequencyPattern creates a pattern matcher for event frequency
func FrequencyPattern(minEvents int, withinDuration time.Duration) PatternMatcherFunc {
	return func(events []*Event) bool {
		if len(events) < minEvents {
			return false
		}
		
		// Check if we have minEvents within the duration
		if len(events) == 0 {
			return false
		}
		
		startTime := events[0].Timestamp
		endTime := events[len(events)-1].Timestamp
		
		return len(events) >= minEvents && endTime.Sub(startTime) <= withinDuration
	}
}