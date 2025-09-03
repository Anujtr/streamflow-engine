package stream

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewJoinBuffer(t *testing.T) {
	maxSize := 100
	jb := NewJoinBuffer(maxSize)

	if jb == nil {
		t.Fatal("NewJoinBuffer returned nil")
	}

	if jb.maxSize != maxSize {
		t.Errorf("Expected max size %d, got %d", maxSize, jb.maxSize)
	}

	if len(jb.events) != 0 {
		t.Errorf("Expected 0 events initially, got %d", len(jb.events))
	}
}

func TestJoinBuffer_AddEvent(t *testing.T) {
	jb := NewJoinBuffer(10)

	event := &Event{
		Key:       "test-key",
		Timestamp: time.Now(),
	}

	window := &Window{
		Start: time.Now(),
		End:   time.Now().Add(time.Minute),
	}

	err := jb.AddEvent("join-key", event, window)
	if err != nil {
		t.Errorf("AddEvent should not error: %v", err)
	}

	// Check that event was added
	events := jb.events["join-key"]
	if len(events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(events))
	}

	if events[0] != event {
		t.Error("Added event should match original")
	}
}

func TestJoinBuffer_AddEvent_BufferFull(t *testing.T) {
	jb := NewJoinBuffer(1) // Very small buffer

	event1 := &Event{Key: "key1", Timestamp: time.Now()}
	event2 := &Event{Key: "key2", Timestamp: time.Now()}
	window := &Window{Start: time.Now(), End: time.Now().Add(time.Minute)}

	// First event should succeed
	err := jb.AddEvent("join-key1", event1, window)
	if err != nil {
		t.Errorf("First AddEvent should not error: %v", err)
	}

	// Second event should fail (buffer full)
	err = jb.AddEvent("join-key2", event2, window)
	if err == nil {
		t.Error("AddEvent should error when buffer is full")
	}
}

func TestJoinBuffer_GetEvents(t *testing.T) {
	jb := NewJoinBuffer(10)

	now := time.Now()
	window := &Window{Start: now, End: now.Add(5 * time.Minute)}

	// Add events with different timestamps
	event1 := &Event{Key: "key1", Timestamp: now.Add(1 * time.Minute)}
	event2 := &Event{Key: "key1", Timestamp: now.Add(10 * time.Minute)} // Outside window
	event3 := &Event{Key: "key1", Timestamp: now.Add(2 * time.Minute)}

	jb.AddEvent("join-key", event1, window)
	jb.AddEvent("join-key", event2, window)
	jb.AddEvent("join-key", event3, window)

	// Get events within the window
	matchingEvents := jb.GetEvents("join-key", window)

	// Should get events 1 and 3 (event2 is outside window)
	if len(matchingEvents) != 2 {
		t.Errorf("Expected 2 matching events, got %d", len(matchingEvents))
	}

	// Verify correct events are returned
	found1, found3 := false, false
	for _, event := range matchingEvents {
		if event == event1 {
			found1 = true
		} else if event == event3 {
			found3 = true
		}
	}

	if !found1 || !found3 {
		t.Error("Should find event1 and event3 within window")
	}
}

func TestJoinBuffer_CleanupExpired(t *testing.T) {
	jb := NewJoinBuffer(10)

	now := time.Now()
	window := &Window{Start: now, End: now.Add(time.Minute)}

	// Add events with different timestamps
	oldEvent := &Event{Key: "old", Timestamp: now.Add(-10 * time.Minute)}
	newEvent := &Event{Key: "new", Timestamp: now}

	jb.AddEvent("join-key", oldEvent, window)
	jb.AddEvent("join-key", newEvent, window)

	// Initially should have 2 events
	if len(jb.events["join-key"]) != 2 {
		t.Errorf("Expected 2 events initially, got %d", len(jb.events["join-key"]))
	}

	// Cleanup expired events
	cutoffTime := now.Add(-5 * time.Minute)
	jb.CleanupExpired(cutoffTime)

	// Should have 1 event remaining (newEvent)
	events := jb.events["join-key"]
	if len(events) != 1 {
		t.Errorf("Expected 1 event after cleanup, got %d", len(events))
	}

	if events[0] != newEvent {
		t.Error("Remaining event should be the new event")
	}
}

func TestNewStreamJoiner(t *testing.T) {
	config := &JoinConfig{
		Type:           InnerJoin,
		WindowSize:     5 * time.Minute,
		MaxBufferSize:  1000,
		LeftKeyFunc:    func(e *Event) string { return e.Key },
		RightKeyFunc:   func(e *Event) string { return e.Key },
	}

	joiner := NewStreamJoiner(config)
	if joiner == nil {
		t.Fatal("NewStreamJoiner returned nil")
	}

	if joiner.config.Type != InnerJoin {
		t.Errorf("Expected join type %v, got %v", InnerJoin, joiner.config.Type)
	}

	if joiner.config.MaxBufferSize != 1000 {
		t.Errorf("Expected buffer size 1000, got %d", joiner.config.MaxBufferSize)
	}
}

func TestNewStreamJoiner_DefaultBufferSize(t *testing.T) {
	config := &JoinConfig{
		Type:       InnerJoin,
		WindowSize: 5 * time.Minute,
		// MaxBufferSize not set
	}

	joiner := NewStreamJoiner(config)
	if joiner.config.MaxBufferSize != 10000 {
		t.Errorf("Expected default buffer size 10000, got %d", joiner.config.MaxBufferSize)
	}
}

func TestStreamJoiner_ProcessLeftEvent(t *testing.T) {
	config := &JoinConfig{
		Type:           InnerJoin,
		WindowSize:     5 * time.Minute,
		MaxBufferSize:  100,
		LeftKeyFunc:    func(e *Event) string { return e.Key },
		RightKeyFunc:   func(e *Event) string { return e.Key },
	}

	joiner := NewStreamJoiner(config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := joiner.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start joiner: %v", err)
	}
	defer joiner.Stop()

	// Process left event
	leftEvent := &Event{
		Key:       "join-key",
		Timestamp: time.Now(),
	}

	err = joiner.ProcessLeftEvent(leftEvent)
	if err != nil {
		t.Errorf("ProcessLeftEvent should not error: %v", err)
	}

	// Should have event in left buffer - use the same window as ProcessLeftEvent creates
	window := &Window{
		Start: leftEvent.Timestamp.Add(-config.WindowSize),
		End:   leftEvent.Timestamp.Add(time.Nanosecond),
	}
	leftEvents := joiner.leftBuffer.GetEvents("join-key", window)

	if len(leftEvents) != 1 {
		t.Errorf("Expected 1 event in left buffer, got %d", len(leftEvents))
	}
}

func TestStreamJoiner_ProcessRightEvent(t *testing.T) {
	config := &JoinConfig{
		Type:           InnerJoin,
		WindowSize:     5 * time.Minute,
		MaxBufferSize:  100,
		LeftKeyFunc:    func(e *Event) string { return e.Key },
		RightKeyFunc:   func(e *Event) string { return e.Key },
	}

	joiner := NewStreamJoiner(config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := joiner.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start joiner: %v", err)
	}
	defer joiner.Stop()

	// Process right event
	rightEvent := &Event{
		Key:       "join-key",
		Timestamp: time.Now(),
	}

	err = joiner.ProcessRightEvent(rightEvent)
	if err != nil {
		t.Errorf("ProcessRightEvent should not error: %v", err)
	}

	// Should have event in right buffer - use the same window as ProcessRightEvent creates
	window := &Window{
		Start: rightEvent.Timestamp.Add(-config.WindowSize),
		End:   rightEvent.Timestamp.Add(time.Nanosecond),
	}
	rightEvents := joiner.rightBuffer.GetEvents("join-key", window)

	if len(rightEvents) != 1 {
		t.Errorf("Expected 1 event in right buffer, got %d", len(rightEvents))
	}
}

func TestStreamJoiner_InnerJoin(t *testing.T) {
	config := &JoinConfig{
		Type:           InnerJoin,
		WindowSize:     5 * time.Minute,
		MaxBufferSize:  100,
		LeftKeyFunc:    func(e *Event) string { return e.Key },
		RightKeyFunc:   func(e *Event) string { return e.Key },
	}

	joiner := NewStreamJoiner(config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := joiner.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start joiner: %v", err)
	}
	defer joiner.Stop()

	now := time.Now()

	// Add right event first
	rightEvent := &Event{
		Key:       "join-key",
		Value:     []byte("right-value"),
		Timestamp: now,
	}

	err = joiner.ProcessRightEvent(rightEvent)
	if err != nil {
		t.Errorf("ProcessRightEvent should not error: %v", err)
	}

	// Add left event that should join
	leftEvent := &Event{
		Key:       "join-key",
		Value:     []byte("left-value"),
		Timestamp: now.Add(1 * time.Minute),
	}

	err = joiner.ProcessLeftEvent(leftEvent)
	if err != nil {
		t.Errorf("ProcessLeftEvent should not error: %v", err)
	}

	// Should have a join result
	select {
	case result := <-joiner.GetResults():
		if result.LeftEvent != leftEvent {
			t.Error("Join result should contain left event")
		}
		if result.RightEvent != rightEvent {
			t.Error("Join result should contain right event")
		}
		if result.JoinKey != "join-key" {
			t.Errorf("Expected join key 'join-key', got '%s'", result.JoinKey)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Should have received join result")
	}
}

func TestStreamJoiner_LeftJoin(t *testing.T) {
	config := &JoinConfig{
		Type:           LeftJoin,
		WindowSize:     5 * time.Minute,
		MaxBufferSize:  100,
		LeftKeyFunc:    func(e *Event) string { return e.Key },
		RightKeyFunc:   func(e *Event) string { return e.Key },
	}

	joiner := NewStreamJoiner(config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := joiner.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start joiner: %v", err)
	}
	defer joiner.Stop()

	// Add left event with no matching right event
	leftEvent := &Event{
		Key:       "left-only-key",
		Value:     []byte("left-value"),
		Timestamp: time.Now(),
	}

	err = joiner.ProcessLeftEvent(leftEvent)
	if err != nil {
		t.Errorf("ProcessLeftEvent should not error: %v", err)
	}

	// Should have a join result with null right event
	select {
	case result := <-joiner.GetResults():
		if result.LeftEvent != leftEvent {
			t.Error("Join result should contain left event")
		}
		if result.RightEvent != nil {
			t.Error("Join result should have null right event for left join with no match")
		}
		if result.JoinKey != "left-only-key" {
			t.Errorf("Expected join key 'left-only-key', got '%s'", result.JoinKey)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Should have received join result for left join")
	}
}

func TestStreamJoiner_StartStop(t *testing.T) {
	config := &JoinConfig{
		Type:       InnerJoin,
		WindowSize: 5 * time.Minute,
	}

	joiner := NewStreamJoiner(config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the joiner
	err := joiner.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start joiner: %v", err)
	}

	// Starting again should be safe
	err = joiner.Start(ctx)
	if err != nil {
		t.Errorf("Starting already running joiner should not error: %v", err)
	}

	// Stop the joiner
	err = joiner.Stop()
	if err != nil {
		t.Fatalf("Failed to stop joiner: %v", err)
	}

	// Stopping again should be safe
	err = joiner.Stop()
	if err != nil {
		t.Errorf("Stopping already stopped joiner should not error: %v", err)
	}
}

func TestStreamJoiner_ProcessEvent_NotRunning(t *testing.T) {
	config := &JoinConfig{
		Type:       InnerJoin,
		WindowSize: 5 * time.Minute,
	}

	joiner := NewStreamJoiner(config)

	event := &Event{Key: "test", Timestamp: time.Now()}

	// Process event when not running should error
	err := joiner.ProcessLeftEvent(event)
	if err == nil {
		t.Error("ProcessLeftEvent should error when joiner not running")
	}

	err = joiner.ProcessRightEvent(event)
	if err == nil {
		t.Error("ProcessRightEvent should error when joiner not running")
	}
}

func TestNewStreamEnricher(t *testing.T) {
	// Mock enrichment source
	source := &mockEnrichmentSource{
		data: map[string][]byte{
			"key1": []byte(`{"info": "enriched data"}`),
		},
	}

	keyExtractor := func(e *Event) string {
		return e.Key
	}

	enricher := NewStreamEnricher(source, keyExtractor)
	if enricher == nil {
		t.Fatal("NewStreamEnricher returned nil")
	}

	if enricher.source != source {
		t.Error("Enrichment source not set correctly")
	}
}

func TestStreamEnricher_Enrich(t *testing.T) {
	// Mock enrichment source
	source := &mockEnrichmentSource{
		data: map[string][]byte{
			"test-key": []byte(`{"category": "premium", "score": 95}`),
		},
	}

	keyExtractor := func(e *Event) string {
		return e.Key
	}

	enricher := NewStreamEnricher(source, keyExtractor)

	event := &Event{
		Key:      "test-key",
		Value:    []byte("original-value"),
		Metadata: make(map[string]interface{}),
	}

	enrichedEvent, err := enricher.Enrich(event)
	if err != nil {
		t.Errorf("Enrich should not error: %v", err)
	}

	// Should be a different instance (cloned)
	if enrichedEvent == event {
		t.Error("Enriched event should be a clone")
	}

	// Should have enrichment data
	if enrichedEvent.Metadata["enrichment"] == nil {
		t.Error("Enriched event should have enrichment metadata")
	}

	enrichmentData := enrichedEvent.Metadata["enrichment"].(map[string]interface{})
	if enrichmentData["category"] != "premium" {
		t.Errorf("Expected category 'premium', got %v", enrichmentData["category"])
	}
}

func TestStreamEnricher_Enrich_LookupError(t *testing.T) {
	// Mock enrichment source that always errors
	source := &mockEnrichmentSource{
		shouldError: true,
	}

	keyExtractor := func(e *Event) string {
		return e.Key
	}

	enricher := NewStreamEnricher(source, keyExtractor)

	event := &Event{
		Key:   "test-key",
		Value: []byte("original-value"),
	}

	enrichedEvent, err := enricher.Enrich(event)
	if err == nil {
		t.Error("Enrich should error when lookup fails")
	}

	// Should return original event on error
	if enrichedEvent != event {
		t.Error("Should return original event on lookup error")
	}
}

func TestStreamEnricher_Close(t *testing.T) {
	source := &mockEnrichmentSource{
		data: make(map[string][]byte),
	}

	enricher := NewStreamEnricher(source, nil)

	err := enricher.Close()
	if err != nil {
		t.Errorf("Close should not error: %v", err)
	}

	// Verify source was closed
	if !source.closed {
		t.Error("Enrichment source should be closed")
	}
}

// Mock enrichment source for testing
type mockEnrichmentSource struct {
	data        map[string][]byte
	shouldError bool
	closed      bool
}

func (m *mockEnrichmentSource) Lookup(key string) ([]byte, error) {
	if m.shouldError {
		return nil, errors.New("lookup error")
	}

	if data, exists := m.data[key]; exists {
		return data, nil
	}

	return nil, errors.New("key not found")
}

func (m *mockEnrichmentSource) Close() error {
	m.closed = true
	return nil
}