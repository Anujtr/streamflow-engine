package stream

import (
	"context"
	"testing"
	"time"
)

func TestNewWatermarkManager(t *testing.T) {
	config := &WatermarkConfig{
		MaxOutOfOrderness: 5 * time.Second,
		IdleSourceTimeout: 30 * time.Second,
		WatermarkInterval: 1 * time.Second,
	}

	wm := NewWatermarkManager(config)
	if wm == nil {
		t.Fatal("NewWatermarkManager returned nil")
	}

	// Test with nil config (should use defaults)
	wm2 := NewWatermarkManager(nil)
	if wm2 == nil {
		t.Fatal("NewWatermarkManager with nil config returned nil")
	}
}

func TestWatermarkManager_UpdateWatermark(t *testing.T) {
	wm := NewWatermarkManager(&WatermarkConfig{
		MaxOutOfOrderness: 5 * time.Second,
		IdleSourceTimeout: 30 * time.Second,
		WatermarkInterval: 1 * time.Second,
	})

	now := time.Now()
	
	// Update watermark for source1
	wm.UpdateWatermark("source1", now)
	
	watermark := wm.GetWatermark("source1")
	if watermark == nil {
		t.Fatal("Expected watermark for source1")
	}
	
	if watermark.Source != "source1" {
		t.Errorf("Expected source 'source1', got '%s'", watermark.Source)
	}
	
	// Watermark should be timestamp minus max out-of-orderness
	expectedTime := now.Add(-5 * time.Second)
	if !watermark.Timestamp.Equal(expectedTime) {
		t.Errorf("Expected watermark timestamp %v, got %v", expectedTime, watermark.Timestamp)
	}
}

func TestWatermarkManager_GetWatermark(t *testing.T) {
	wm := NewWatermarkManager(&WatermarkConfig{
		MaxOutOfOrderness: 5 * time.Second,
		IdleSourceTimeout: 30 * time.Second,
		WatermarkInterval: 1 * time.Second,
	})

	// Get watermark for non-existent source
	watermark := wm.GetWatermark("nonexistent")
	if watermark != nil {
		t.Error("Expected nil watermark for non-existent source")
	}

	// Update and get watermark
	now := time.Now()
	wm.UpdateWatermark("source1", now)
	
	watermark = wm.GetWatermark("source1")
	if watermark == nil {
		t.Fatal("Expected watermark for source1")
	}
	
	if watermark.Source != "source1" {
		t.Errorf("Expected source 'source1', got '%s'", watermark.Source)
	}
}

func TestWatermarkManager_GetGlobalWatermark(t *testing.T) {
	wm := NewWatermarkManager(&WatermarkConfig{
		MaxOutOfOrderness: 5 * time.Second,
		IdleSourceTimeout: 30 * time.Second,
		WatermarkInterval: 1 * time.Second,
	})

	// Initially no global watermark
	globalWatermark := wm.GetGlobalWatermark()
	if globalWatermark != nil {
		t.Error("Expected nil global watermark initially")
	}

	now := time.Now()
	
	// Add watermark for source1
	wm.UpdateWatermark("source1", now)
	globalWatermark = wm.GetGlobalWatermark()
	if globalWatermark == nil {
		t.Fatal("Expected global watermark after adding source1")
	}
	
	if globalWatermark.Source != "global" {
		t.Errorf("Expected global source, got '%s'", globalWatermark.Source)
	}

	// Add watermark for source2 with earlier timestamp
	earlier := now.Add(-10 * time.Second)
	wm.UpdateWatermark("source2", earlier)
	
	globalWatermark = wm.GetGlobalWatermark()
	if globalWatermark == nil {
		t.Fatal("Expected global watermark after adding source2")
	}
	
	// Global watermark should be the minimum (source2's watermark)
	expectedTime := earlier.Add(-5 * time.Second)
	if !globalWatermark.Timestamp.Equal(expectedTime) {
		t.Errorf("Expected global watermark %v, got %v", expectedTime, globalWatermark.Timestamp)
	}

	// Add watermark for source3 with later timestamp
	later := now.Add(10 * time.Second)
	wm.UpdateWatermark("source3", later)
	
	globalWatermark = wm.GetGlobalWatermark()
	// Global watermark should still be source2's (minimum)
	if !globalWatermark.Timestamp.Equal(expectedTime) {
		t.Errorf("Expected global watermark to remain %v, got %v", expectedTime, globalWatermark.Timestamp)
	}
}

func TestWatermarkManager_IsLateEvent(t *testing.T) {
	wm := NewWatermarkManager(&WatermarkConfig{
		MaxOutOfOrderness: 5 * time.Second,
		IdleSourceTimeout: 30 * time.Second,
		WatermarkInterval: 1 * time.Second,
	})

	now := time.Now()
	event := &Event{
		Key:       "test",
		Timestamp: now,
	}

	// No watermark established yet
	isLate := wm.IsLateEvent(event, 2*time.Second)
	if isLate {
		t.Error("Event should not be late when no watermark established")
	}

	// Establish watermark
	wm.UpdateWatermark("source1", now.Add(10*time.Second))

	// Event with timestamp before watermark minus allowed lateness should be late
	event.Timestamp = now.Add(-10 * time.Second) // Very old event
	isLate = wm.IsLateEvent(event, 2*time.Second)
	if !isLate {
		t.Error("Very old event should be considered late")
	}

	// Event with recent timestamp should not be late
	event.Timestamp = now.Add(5 * time.Second)
	isLate = wm.IsLateEvent(event, 2*time.Second)
	if isLate {
		t.Error("Recent event should not be considered late")
	}
}

func TestWatermarkManager_HandleLateEvent(t *testing.T) {
	wm := NewWatermarkManager(&WatermarkConfig{
		MaxOutOfOrderness: 5 * time.Second,
		IdleSourceTimeout: 30 * time.Second,
		WatermarkInterval: 1 * time.Second,
	})

	event := &Event{
		Key:       "test",
		Timestamp: time.Now(),
	}

	// Should not error (basic implementation just returns nil)
	err := wm.HandleLateEvent(event)
	if err != nil {
		t.Errorf("HandleLateEvent should not error: %v", err)
	}
}

func TestWatermarkManager_StartStop(t *testing.T) {
	wm := NewWatermarkManager(&WatermarkConfig{
		MaxOutOfOrderness: 5 * time.Second,
		IdleSourceTimeout: 30 * time.Second,
		WatermarkInterval: 100 * time.Millisecond, // Fast for testing
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the watermark manager
	err := wm.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start watermark manager: %v", err)
	}

	// Starting again should be safe
	err = wm.Start(ctx)
	if err != nil {
		t.Errorf("Starting already running watermark manager should not error: %v", err)
	}

	// Stop the watermark manager
	err = wm.Stop()
	if err != nil {
		t.Fatalf("Failed to stop watermark manager: %v", err)
	}

	// Stopping again should be safe
	err = wm.Stop()
	if err != nil {
		t.Errorf("Stopping already stopped watermark manager should not error: %v", err)
	}
}

func TestWatermarkManager_WatermarkAdvancement(t *testing.T) {
	wm := NewWatermarkManager(&WatermarkConfig{
		MaxOutOfOrderness: 1 * time.Second,
		IdleSourceTimeout: 2 * time.Second,
		WatermarkInterval: 100 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := wm.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start watermark manager: %v", err)
	}
	defer wm.Stop()

	now := time.Now()
	wm.UpdateWatermark("source1", now)

	initialWatermark := wm.GetWatermark("source1")
	if initialWatermark == nil {
		t.Fatal("Expected initial watermark")
	}

	// Wait for watermark advancement
	time.Sleep(300 * time.Millisecond)

	updatedWatermark := wm.GetWatermark("source1")
	if updatedWatermark == nil {
		t.Fatal("Expected updated watermark")
	}

	// Watermark should have advanced
	if !updatedWatermark.Timestamp.After(initialWatermark.Timestamp) {
		t.Errorf("Watermark should have advanced: initial=%v, updated=%v",
			initialWatermark.Timestamp, updatedWatermark.Timestamp)
	}
}

func TestNewEventTimeExtractor(t *testing.T) {
	timeExtractor := func(e *Event) time.Time {
		return e.Timestamp.Add(1 * time.Hour) // Add one hour
	}

	extractor := NewEventTimeExtractor(timeExtractor)
	if extractor == nil {
		t.Fatal("NewEventTimeExtractor returned nil")
	}

	event := &Event{
		Timestamp: time.Now(),
	}

	extractedTime := extractor.ExtractEventTime(event)
	expectedTime := event.Timestamp.Add(1 * time.Hour)

	if !extractedTime.Equal(expectedTime) {
		t.Errorf("Expected extracted time %v, got %v", expectedTime, extractedTime)
	}
}

func TestEventTimeExtractor_ExtractEventTime(t *testing.T) {
	// Test with custom extractor
	timeExtractor := func(e *Event) time.Time {
		return e.Timestamp.Add(2 * time.Hour)
	}

	extractor := NewEventTimeExtractor(timeExtractor)
	event := &Event{Timestamp: time.Now()}

	extractedTime := extractor.ExtractEventTime(event)
	expectedTime := event.Timestamp.Add(2 * time.Hour)

	if !extractedTime.Equal(expectedTime) {
		t.Errorf("Expected extracted time %v, got %v", expectedTime, extractedTime)
	}

	// Test without custom extractor (should use event timestamp)
	extractor2 := NewEventTimeExtractor(nil)
	extractedTime2 := extractor2.ExtractEventTime(event)

	if !extractedTime2.Equal(event.Timestamp) {
		t.Errorf("Expected extracted time %v, got %v", event.Timestamp, extractedTime2)
	}

	// Test with zero timestamp (should fallback to processing time)
	event3 := &Event{Timestamp: time.Time{}}
	extractedTime3 := extractor2.ExtractEventTime(event3)

	if extractedTime3.IsZero() {
		t.Error("Expected fallback to processing time, got zero time")
	}
}

func TestNewWatermarkGenerator(t *testing.T) {
	wm := NewWatermarkManager(nil)
	extractor := NewEventTimeExtractor(nil)
	generator := NewWatermarkGenerator(extractor, wm)

	if generator == nil {
		t.Fatal("NewWatermarkGenerator returned nil")
	}
}

func TestWatermarkGenerator_ProcessEvent(t *testing.T) {
	wm := NewWatermarkManager(&WatermarkConfig{
		MaxOutOfOrderness: 5 * time.Second,
		IdleSourceTimeout: 30 * time.Second,
		WatermarkInterval: 1 * time.Second,
	})
	extractor := NewEventTimeExtractor(nil)
	generator := NewWatermarkGenerator(extractor, wm)

	event := &Event{
		Key:       "test",
		Timestamp: time.Now(),
	}

	watermark := generator.ProcessEvent(event, "source1")
	if watermark == nil {
		t.Fatal("Expected watermark from ProcessEvent")
	}

	if watermark.Source != "source1" {
		t.Errorf("Expected source 'source1', got '%s'", watermark.Source)
	}

	// Global watermark should also be available
	globalWatermark := generator.GetGlobalWatermark()
	if globalWatermark == nil {
		t.Fatal("Expected global watermark")
	}
}

func TestNewLateEventHandler(t *testing.T) {
	wm := NewWatermarkManager(nil)
	handler := NewLateEventHandler(wm, 100)

	if handler == nil {
		t.Fatal("NewLateEventHandler returned nil")
	}
}

func TestLateEventHandler_HandleEvent(t *testing.T) {
	wm := NewWatermarkManager(&WatermarkConfig{
		MaxOutOfOrderness: 5 * time.Second,
		IdleSourceTimeout: 30 * time.Second,
		WatermarkInterval: 1 * time.Second,
	})
	handler := NewLateEventHandler(wm, 100)

	// Establish watermark
	now := time.Now()
	wm.UpdateWatermark("source1", now)

	// Test non-late event (recent timestamp that should not be late)
	event := &Event{
		Key:       "test",
		Timestamp: now.Add(-2 * time.Second), // Only 2 seconds old, within allowed lateness
	}

	isLate, err := handler.HandleEvent(event, 10*time.Second) // Allow 10 seconds of lateness
	if err != nil {
		t.Errorf("HandleEvent should not error: %v", err)
	}
	if isLate {
		t.Error("Event should not be late")
	}

	// Test late event (use a different key to avoid mixing with previous event)
	lateEvent := &Event{
		Key:       "test-late",
		Timestamp: now.Add(-20 * time.Second), // Very old
	}

	isLate, err = handler.HandleEvent(lateEvent, 2*time.Second)
	if err != nil {
		t.Errorf("HandleEvent should not error: %v", err)
	}
	if !isLate {
		t.Error("Event should be late")
	}

	// Should be able to retrieve late events (use the same key as the late event)
	lateEvents := handler.GetLateEvents("test-late")
	if len(lateEvents) != 1 {
		t.Errorf("Expected 1 late event, got %d", len(lateEvents))
	}
}

func TestLateEventHandler_GetLateEvents(t *testing.T) {
	wm := NewWatermarkManager(nil)
	handler := NewLateEventHandler(wm, 100)

	// No late events initially
	lateEvents := handler.GetLateEvents("test")
	if len(lateEvents) != 0 {
		t.Errorf("Expected 0 late events initially, got %d", len(lateEvents))
	}

	// Manually add late event to test retrieval
	wm.UpdateWatermark("source1", time.Now().Add(10*time.Second))
	lateEvent := &Event{
		Key:       "test",
		Timestamp: time.Now().Add(-30 * time.Second),
	}

	isLate, err := handler.HandleEvent(lateEvent, 1*time.Second)
	if err != nil {
		t.Errorf("HandleEvent should not error: %v", err)
	}
	if !isLate {
		t.Error("Event should be late")
	}

	lateEvents = handler.GetLateEvents("test")
	if len(lateEvents) != 1 {
		t.Errorf("Expected 1 late event, got %d", len(lateEvents))
	}

	if lateEvents[0] != lateEvent {
		t.Error("Retrieved late event should match original")
	}
}

func TestLateEventHandler_ClearLateEvents(t *testing.T) {
	wm := NewWatermarkManager(nil)
	handler := NewLateEventHandler(wm, 100)

	// Add a late event
	wm.UpdateWatermark("source1", time.Now().Add(10*time.Second))
	lateEvent := &Event{
		Key:       "test",
		Timestamp: time.Now().Add(-30 * time.Second),
	}

	handler.HandleEvent(lateEvent, 1*time.Second)

	// Verify event was buffered
	lateEvents := handler.GetLateEvents("test")
	if len(lateEvents) != 1 {
		t.Errorf("Expected 1 late event, got %d", len(lateEvents))
	}

	// Clear late events
	handler.ClearLateEvents("test")

	// Should be no late events now
	lateEvents = handler.GetLateEvents("test")
	if len(lateEvents) != 0 {
		t.Errorf("Expected 0 late events after clear, got %d", len(lateEvents))
	}
}