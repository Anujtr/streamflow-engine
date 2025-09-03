package stream

import (
	"context"
	"testing"
	"time"
)

func TestNewPatternDetector(t *testing.T) {
	pd := NewPatternDetector()
	if pd == nil {
		t.Fatal("NewPatternDetector returned nil")
	}
}

func TestPatternDetector_AddPattern(t *testing.T) {
	pd := NewPatternDetector()

	matcher := func(events []*Event) bool {
		return len(events) >= 2
	}

	err := pd.AddPattern("test-pattern", matcher, 5*time.Minute)
	if err != nil {
		t.Errorf("AddPattern should not error: %v", err)
	}

	// Adding same pattern again should error
	err = pd.AddPattern("test-pattern", matcher, 5*time.Minute)
	if err == nil {
		t.Error("Adding duplicate pattern should error")
	}
}

func TestPatternDetector_RemovePattern(t *testing.T) {
	pd := NewPatternDetector()

	matcher := func(events []*Event) bool {
		return len(events) >= 2
	}

	// Remove non-existent pattern should error
	err := pd.RemovePattern("nonexistent")
	if err == nil {
		t.Error("Removing non-existent pattern should error")
	}

	// Add and remove pattern
	err = pd.AddPattern("test-pattern", matcher, 5*time.Minute)
	if err != nil {
		t.Errorf("AddPattern should not error: %v", err)
	}

	err = pd.RemovePattern("test-pattern")
	if err != nil {
		t.Errorf("RemovePattern should not error: %v", err)
	}

	// Remove again should error
	err = pd.RemovePattern("test-pattern")
	if err == nil {
		t.Error("Removing already removed pattern should error")
	}
}

func TestPatternDetector_ProcessEvent(t *testing.T) {
	pd := NewPatternDetector()

	// Pattern that matches when we have 2 events
	matcher := func(events []*Event) bool {
		return len(events) == 2
	}

	err := pd.AddPattern("two-events", matcher, 5*time.Minute)
	if err != nil {
		t.Fatalf("AddPattern failed: %v", err)
	}

	now := time.Now()

	// First event - should not match
	event1 := &Event{
		Key:       "test-key",
		Timestamp: now,
	}

	results, err := pd.ProcessEvent(event1)
	if err != nil {
		t.Errorf("ProcessEvent should not error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}

	// Second event - should match
	event2 := &Event{
		Key:       "test-key",
		Timestamp: now.Add(1 * time.Minute),
	}

	results, err = pd.ProcessEvent(event2)
	if err != nil {
		t.Errorf("ProcessEvent should not error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	result := results[0]
	if result.PatternName != "two-events" {
		t.Errorf("Expected pattern name 'two-events', got '%s'", result.PatternName)
	}
	if len(result.Events) != 2 {
		t.Errorf("Expected 2 events in result, got %d", len(result.Events))
	}
	if result.StartTime != event1.Timestamp {
		t.Errorf("Expected start time %v, got %v", event1.Timestamp, result.StartTime)
	}
	if result.EndTime != event2.Timestamp {
		t.Errorf("Expected end time %v, got %v", event2.Timestamp, result.EndTime)
	}
}

func TestPatternDetector_ProcessEventDifferentKeys(t *testing.T) {
	pd := NewPatternDetector()

	matcher := func(events []*Event) bool {
		return len(events) == 2
	}

	err := pd.AddPattern("test-pattern", matcher, 5*time.Minute)
	if err != nil {
		t.Fatalf("AddPattern failed: %v", err)
	}

	now := time.Now()

	// Events with different keys should be tracked separately
	event1 := &Event{Key: "key1", Timestamp: now}
	event2 := &Event{Key: "key2", Timestamp: now}
	event3 := &Event{Key: "key1", Timestamp: now.Add(1 * time.Minute)}

	// First two events with different keys - no match
	pd.ProcessEvent(event1)
	results, _ := pd.ProcessEvent(event2)
	if len(results) != 0 {
		t.Errorf("Expected 0 results for different keys, got %d", len(results))
	}

	// Third event with same key as first - should match
	results, _ = pd.ProcessEvent(event3)
	if len(results) != 1 {
		t.Errorf("Expected 1 result for matching key, got %d", len(results))
	}

	if results[0].Events[0].Key != "key1" || results[0].Events[1].Key != "key1" {
		t.Error("Result should contain events with same key")
	}
}

func TestPatternDetector_ExpiredPattern(t *testing.T) {
	pd := NewPatternDetector()

	matcher := func(events []*Event) bool {
		return len(events) == 2
	}

	// Very short timeout for testing
	err := pd.AddPattern("test-pattern", matcher, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("AddPattern failed: %v", err)
	}

	now := time.Now()

	// First event
	event1 := &Event{Key: "test-key", Timestamp: now}
	pd.ProcessEvent(event1)

	// Wait for pattern to expire
	time.Sleep(150 * time.Millisecond)

	// Second event (after expiration) should start new pattern
	event2 := &Event{Key: "test-key", Timestamp: now.Add(200 * time.Millisecond)}
	results, _ := pd.ProcessEvent(event2)

	// Should not match because pattern expired
	if len(results) != 0 {
		t.Errorf("Expected 0 results for expired pattern, got %d", len(results))
	}

	// Third event should complete the new pattern
	event3 := &Event{Key: "test-key", Timestamp: now.Add(250 * time.Millisecond)}
	results, _ = pd.ProcessEvent(event3)

	if len(results) != 1 {
		t.Errorf("Expected 1 result for new pattern, got %d", len(results))
	}

	// Result should contain events 2 and 3, not 1 and 3
	result := results[0]
	if result.StartTime == event1.Timestamp {
		t.Error("Result should not include expired event1")
	}
	if result.StartTime != event2.Timestamp {
		t.Errorf("Expected start time %v, got %v", event2.Timestamp, result.StartTime)
	}
}

func TestPatternDetector_GetPatternState(t *testing.T) {
	pd := NewPatternDetector()

	matcher := func(events []*Event) bool {
		return false // Never matches
	}

	err := pd.AddPattern("test-pattern", matcher, 5*time.Minute)
	if err != nil {
		t.Fatalf("AddPattern failed: %v", err)
	}

	// Get state for non-existent pattern
	_, err = pd.GetPatternState("nonexistent")
	if err == nil {
		t.Error("GetPatternState for non-existent pattern should error")
	}

	// Get state for existing pattern (no events yet)
	state, err := pd.GetPatternState("test-pattern")
	if err != nil {
		t.Errorf("GetPatternState should not error: %v", err)
	}
	if state["active_keys"] != 0 {
		t.Errorf("Expected 0 active keys, got %v", state["active_keys"])
	}
	if state["total_events"] != 0 {
		t.Errorf("Expected 0 total events, got %v", state["total_events"])
	}

	// Add some events
	event1 := &Event{Key: "key1", Timestamp: time.Now()}
	event2 := &Event{Key: "key2", Timestamp: time.Now()}
	pd.ProcessEvent(event1)
	pd.ProcessEvent(event2)

	state, err = pd.GetPatternState("test-pattern")
	if err != nil {
		t.Errorf("GetPatternState should not error: %v", err)
	}
	if state["active_keys"] != 2 {
		t.Errorf("Expected 2 active keys, got %v", state["active_keys"])
	}
	if state["total_events"] != 2 {
		t.Errorf("Expected 2 total events, got %v", state["total_events"])
	}
}

func TestPatternDetector_ClearPatternState(t *testing.T) {
	pd := NewPatternDetector()

	matcher := func(events []*Event) bool {
		return false // Never matches
	}

	err := pd.AddPattern("test-pattern", matcher, 5*time.Minute)
	if err != nil {
		t.Fatalf("AddPattern failed: %v", err)
	}

	// Clear state for non-existent pattern
	err = pd.ClearPatternState("nonexistent")
	if err == nil {
		t.Error("ClearPatternState for non-existent pattern should error")
	}

	// Add some events
	event := &Event{Key: "key1", Timestamp: time.Now()}
	pd.ProcessEvent(event)

	// Verify state exists
	state, _ := pd.GetPatternState("test-pattern")
	if state["total_events"] != 1 {
		t.Errorf("Expected 1 total event, got %v", state["total_events"])
	}

	// Clear state
	err = pd.ClearPatternState("test-pattern")
	if err != nil {
		t.Errorf("ClearPatternState should not error: %v", err)
	}

	// Verify state is cleared
	state, _ = pd.GetPatternState("test-pattern")
	if state["total_events"] != 0 {
		t.Errorf("Expected 0 total events after clear, got %v", state["total_events"])
	}
}

func TestPatternDetector_StartStop(t *testing.T) {
	pd := NewPatternDetector()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the pattern detector
	err := pd.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start pattern detector: %v", err)
	}

	// Starting again should be safe
	err = pd.Start(ctx)
	if err != nil {
		t.Errorf("Starting already running pattern detector should not error: %v", err)
	}

	// Stop the pattern detector
	err = pd.Stop()
	if err != nil {
		t.Fatalf("Failed to stop pattern detector: %v", err)
	}

	// Stopping again should be safe
	err = pd.Stop()
	if err != nil {
		t.Errorf("Stopping already stopped pattern detector should not error: %v", err)
	}
}

func TestPatternDetector_MaxEvents(t *testing.T) {
	pd := NewPatternDetector()

	// Pattern that never matches
	matcher := func(events []*Event) bool {
		return false
	}

	err := pd.AddPattern("test-pattern", matcher, 5*time.Minute)
	if err != nil {
		t.Fatalf("AddPattern failed: %v", err)
	}

	// Add more than max events (default is 100)
	for i := 0; i < 150; i++ {
		event := &Event{
			Key:       "test-key",
			Timestamp: time.Now(),
		}
		pd.ProcessEvent(event)
	}

	// State should be reset due to max events limit
	state, err := pd.GetPatternState("test-pattern")
	if err != nil {
		t.Errorf("GetPatternState should not error: %v", err)
	}

	// Should have some events but not 150 (due to reset)
	totalEvents := state["total_events"].(int)
	if totalEvents >= 150 {
		t.Errorf("Expected fewer than 150 events due to max limit, got %d", totalEvents)
	}
}

func TestSequencePattern(t *testing.T) {
	matcher := SequencePattern("A", "B", "C")

	// Test matching sequence
	events := []*Event{
		{Key: "A"},
		{Key: "B"},
		{Key: "C"},
	}

	if !matcher(events) {
		t.Error("Sequence A-B-C should match")
	}

	// Test non-matching sequence
	events2 := []*Event{
		{Key: "A"},
		{Key: "C"},
		{Key: "B"},
	}

	if matcher(events2) {
		t.Error("Sequence A-C-B should not match")
	}

	// Test insufficient events
	events3 := []*Event{
		{Key: "A"},
		{Key: "B"},
	}

	if matcher(events3) {
		t.Error("Incomplete sequence A-B should not match A-B-C pattern")
	}

	// Test with metadata
	eventsWithMetadata := []*Event{
		{Key: "other", Metadata: map[string]interface{}{"event_type": "A"}},
		{Key: "other", Metadata: map[string]interface{}{"event_type": "B"}},
		{Key: "other", Metadata: map[string]interface{}{"event_type": "C"}},
	}

	if !matcher(eventsWithMetadata) {
		t.Error("Sequence with metadata should match")
	}
}

func TestThresholdPattern(t *testing.T) {
	valueExtractor := func(e *Event) (float64, error) {
		if e.Metadata != nil {
			if val, exists := e.Metadata["value"]; exists {
				return val.(float64), nil
			}
		}
		return 0, nil
	}

	matcher := ThresholdPattern(valueExtractor, 100, ">")

	// Test threshold exceeded
	events := []*Event{
		{Metadata: map[string]interface{}{"value": 150.0}},
	}

	if !matcher(events) {
		t.Error("Value 150 should exceed threshold 100")
	}

	// Test threshold not exceeded
	events2 := []*Event{
		{Metadata: map[string]interface{}{"value": 50.0}},
	}

	if matcher(events2) {
		t.Error("Value 50 should not exceed threshold 100")
	}

	// Test different operators
	matcherLess := ThresholdPattern(valueExtractor, 100, "<")
	if !matcherLess(events2) {
		t.Error("Value 50 should be less than threshold 100")
	}

	matcherEqual := ThresholdPattern(valueExtractor, 100, "==")
	eventsEqual := []*Event{
		{Metadata: map[string]interface{}{"value": 100.0}},
	}
	if !matcherEqual(eventsEqual) {
		t.Error("Value 100 should equal threshold 100")
	}
}

func TestFrequencyPattern(t *testing.T) {
	matcher := FrequencyPattern(3, 5*time.Minute)

	now := time.Now()
	
	// Test frequency within time window
	events := []*Event{
		{Timestamp: now},
		{Timestamp: now.Add(1 * time.Minute)},
		{Timestamp: now.Add(2 * time.Minute)},
	}

	if !matcher(events) {
		t.Error("3 events within 5 minutes should match frequency pattern")
	}

	// Test insufficient events
	events2 := []*Event{
		{Timestamp: now},
		{Timestamp: now.Add(1 * time.Minute)},
	}

	if matcher(events2) {
		t.Error("2 events should not match frequency pattern requiring 3")
	}

	// Test events outside time window
	events3 := []*Event{
		{Timestamp: now},
		{Timestamp: now.Add(1 * time.Minute)},
		{Timestamp: now.Add(10 * time.Minute)}, // Outside 5-minute window
	}

	if matcher(events3) {
		t.Error("Events spanning 10 minutes should not match 5-minute frequency pattern")
	}

	// Test empty events
	if matcher([]*Event{}) {
		t.Error("Empty events should not match frequency pattern")
	}
}