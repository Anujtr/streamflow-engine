package stream

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestNewMemoryStateStore(t *testing.T) {
	store := NewMemoryStateStore()
	if store == nil {
		t.Fatal("NewMemoryStateStore returned nil")
	}

	if store.data == nil {
		t.Error("Data map not initialized")
	}

	if store.windows == nil {
		t.Error("Windows map not initialized")
	}
}

func TestMemoryStateStore_GetPut(t *testing.T) {
	store := NewMemoryStateStore()
	ctx := context.Background()

	// Test getting non-existent key
	value, err := store.Get(ctx, "non-existent")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if value != nil {
		t.Error("Expected nil for non-existent key")
	}

	// Test putting and getting a value
	testKey := "test-key"
	testValue := []byte("test-value")

	err = store.Put(ctx, testKey, testValue)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	value, err = store.Get(ctx, testKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if value == nil {
		t.Fatal("Expected value, got nil")
	}
	if string(value) != string(testValue) {
		t.Errorf("Expected value %s, got %s", string(testValue), string(value))
	}

	// Verify it's a copy (modifying returned value shouldn't affect stored value)
	value[0] = 'X'
	value2, _ := store.Get(ctx, testKey)
	if value2[0] == 'X' {
		t.Error("Returned value should be a copy")
	}
}

func TestMemoryStateStore_Delete(t *testing.T) {
	store := NewMemoryStateStore()
	ctx := context.Background()

	testKey := "test-key"
	testValue := []byte("test-value")

	// Put a value
	err := store.Put(ctx, testKey, testValue)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify it exists
	value, err := store.Get(ctx, testKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if value == nil {
		t.Error("Value should exist before delete")
	}

	// Delete the value
	err = store.Delete(ctx, testKey)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it no longer exists
	value, err = store.Get(ctx, testKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if value != nil {
		t.Error("Value should not exist after delete")
	}
}

func TestMemoryStateStore_GetRange(t *testing.T) {
	store := NewMemoryStateStore()
	ctx := context.Background()

	// Put test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"other": "other-value",
	}

	for k, v := range testData {
		err := store.Put(ctx, k, []byte(v))
		if err != nil {
			t.Fatalf("Put failed for %s: %v", k, err)
		}
	}

	// Get range key1 to key3 (should include key1, key2, exclude key3)
	result, err := store.GetRange(ctx, "key1", "key3")
	if err != nil {
		t.Fatalf("GetRange failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Expected 2 results, got %d", len(result))
	}

	if result["key1"] == nil || string(result["key1"]) != "value1" {
		t.Error("Expected key1 with value1 in result")
	}

	if result["key2"] == nil || string(result["key2"]) != "value2" {
		t.Error("Expected key2 with value2 in result")
	}

	if result["key3"] != nil {
		t.Error("key3 should be excluded from range")
	}

	if result["other"] != nil {
		t.Error("other should not be in range")
	}
}

func TestMemoryStateStore_WindowOperations(t *testing.T) {
	store := NewMemoryStateStore()
	ctx := context.Background()

	// Create test window
	window := &Window{
		Start: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		End:   time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
	}

	testKey := "test-key"
	testValue := []byte("test-window-value")

	// Test putting window state
	err := store.PutWindowState(ctx, window, testKey, testValue)
	if err != nil {
		t.Fatalf("PutWindowState failed: %v", err)
	}

	// Test getting window state
	value, err := store.GetWindowState(ctx, window, testKey)
	if err != nil {
		t.Fatalf("GetWindowState failed: %v", err)
	}
	if value == nil {
		t.Fatal("Expected value, got nil")
	}
	if string(value) != string(testValue) {
		t.Errorf("Expected value %s, got %s", string(testValue), string(value))
	}

	// Test getting non-existent window state
	value, err = store.GetWindowState(ctx, window, "non-existent")
	if err != nil {
		t.Fatalf("GetWindowState failed: %v", err)
	}
	if value != nil {
		t.Error("Expected nil for non-existent window state")
	}

	// Test deleting window state
	err = store.DeleteWindowState(ctx, window, testKey)
	if err != nil {
		t.Fatalf("DeleteWindowState failed: %v", err)
	}

	// Verify it's deleted
	value, err = store.GetWindowState(ctx, window, testKey)
	if err != nil {
		t.Fatalf("GetWindowState failed: %v", err)
	}
	if value != nil {
		t.Error("Value should not exist after delete")
	}
}

func TestMemoryStateStore_ExpireWindow(t *testing.T) {
	store := NewMemoryStateStore()
	ctx := context.Background()

	// Create test windows
	window1 := &Window{
		Start: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		End:   time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
	}
	window2 := &Window{
		Start: time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
		End:   time.Date(2024, 1, 1, 14, 0, 0, 0, time.UTC),
	}

	// Put data in both windows
	store.PutWindowState(ctx, window1, "key1", []byte("value1"))
	store.PutWindowState(ctx, window1, "key2", []byte("value2"))
	store.PutWindowState(ctx, window2, "key1", []byte("value3"))

	// Verify data exists
	value, _ := store.GetWindowState(ctx, window1, "key1")
	if value == nil {
		t.Error("Data should exist in window1 before expiration")
	}

	// Expire window1
	err := store.ExpireWindow(ctx, window1)
	if err != nil {
		t.Fatalf("ExpireWindow failed: %v", err)
	}

	// Verify window1 data is gone
	value, _ = store.GetWindowState(ctx, window1, "key1")
	if value != nil {
		t.Error("Data should not exist in window1 after expiration")
	}

	value, _ = store.GetWindowState(ctx, window1, "key2")
	if value != nil {
		t.Error("Data should not exist in window1 after expiration")
	}

	// Verify window2 data still exists
	value, _ = store.GetWindowState(ctx, window2, "key1")
	if value == nil {
		t.Error("Data should still exist in window2 after window1 expiration")
	}
}

func TestMemoryStateStore_ListActiveWindows(t *testing.T) {
	store := NewMemoryStateStore()
	ctx := context.Background()

	// Initially no windows
	windows, err := store.ListActiveWindows(ctx)
	if err != nil {
		t.Fatalf("ListActiveWindows failed: %v", err)
	}
	if len(windows) != 0 {
		t.Errorf("Expected 0 windows, got %d", len(windows))
	}

	// Create test windows with data
	window1 := &Window{
		Start: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		End:   time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
	}
	window2 := &Window{
		Start: time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
		End:   time.Date(2024, 1, 1, 14, 0, 0, 0, time.UTC),
	}

	store.PutWindowState(ctx, window1, "key1", []byte("value1"))
	store.PutWindowState(ctx, window2, "key1", []byte("value2"))

	// Should have 2 active windows
	windows, err = store.ListActiveWindows(ctx)
	if err != nil {
		t.Fatalf("ListActiveWindows failed: %v", err)
	}
	if len(windows) != 2 {
		t.Errorf("Expected 2 windows, got %d", len(windows))
	}

	// Verify window times (order doesn't matter)
	foundWindow1 := false
	foundWindow2 := false
	for _, w := range windows {
		if w.Start.Equal(window1.Start) && w.End.Equal(window1.End) {
			foundWindow1 = true
		}
		if w.Start.Equal(window2.Start) && w.End.Equal(window2.End) {
			foundWindow2 = true
		}
	}

	if !foundWindow1 {
		t.Error("window1 not found in active windows")
	}
	if !foundWindow2 {
		t.Error("window2 not found in active windows")
	}
}

func TestMemoryStateStore_Close(t *testing.T) {
	store := NewMemoryStateStore()
	ctx := context.Background()

	// Put some data
	store.Put(ctx, "key1", []byte("value1"))

	// Close the store
	err := store.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify data is cleared
	value, _ := store.Get(ctx, "key1")
	if value != nil {
		t.Error("Data should be cleared after close")
	}
}

func TestMemoryStateStore_Flush(t *testing.T) {
	store := NewMemoryStateStore()

	// Flush should be a no-op for memory store
	err := store.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

func TestWindowState_Update(t *testing.T) {
	window := &Window{
		Start: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		End:   time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
	}

	state := &WindowState{
		Key:       "test-key",
		Window:    window,
		Count:     0,
		Sum:       0,
		Min:       0,
		Max:       0,
		Events:    make([]*Event, 0),
		Metadata:  make(map[string]interface{}),
		UpdatedAt: time.Now(),
	}

	// Create test event
	event := &Event{
		Key:   "test-key",
		Value: []byte("42.5"),
	}
	err := event.SetValueFromJSON(42.5)
	if err != nil {
		t.Fatalf("Failed to set event value: %v", err)
	}

	// Update state with event
	err = state.Update(event, ValueAsFloat64)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify state was updated
	if state.Count != 1 {
		t.Errorf("Expected count 1, got %d", state.Count)
	}
	if state.Sum != 42.5 {
		t.Errorf("Expected sum 42.5, got %f", state.Sum)
	}
	if state.Min != 42.5 {
		t.Errorf("Expected min 42.5, got %f", state.Min)
	}
	if state.Max != 42.5 {
		t.Errorf("Expected max 42.5, got %f", state.Max)
	}
	if len(state.Events) != 1 {
		t.Errorf("Expected 1 event stored, got %d", len(state.Events))
	}

	// Update with another event
	event2 := &Event{
		Key:   "test-key",
		Value: []byte("10.0"),
	}
	err = event2.SetValueFromJSON(10.0)
	if err != nil {
		t.Fatalf("Failed to set event2 value: %v", err)
	}

	err = state.Update(event2, ValueAsFloat64)
	if err != nil {
		t.Fatalf("Second update failed: %v", err)
	}

	// Verify state was updated correctly
	if state.Count != 2 {
		t.Errorf("Expected count 2, got %d", state.Count)
	}
	if state.Sum != 52.5 {
		t.Errorf("Expected sum 52.5, got %f", state.Sum)
	}
	if state.Min != 10.0 {
		t.Errorf("Expected min 10.0, got %f", state.Min)
	}
	if state.Max != 42.5 {
		t.Errorf("Expected max 42.5, got %f", state.Max)
	}
	if len(state.Events) != 2 {
		t.Errorf("Expected 2 events stored, got %d", len(state.Events))
	}
}

func TestWindowState_GetAverage(t *testing.T) {
	state := &WindowState{
		Count: 0,
		Sum:   0,
	}

	// Test average with no events
	avg := state.GetAverage()
	if avg != 0 {
		t.Errorf("Expected average 0 for empty state, got %f", avg)
	}

	// Test average with events
	state.Count = 3
	state.Sum = 15.0

	avg = state.GetAverage()
	if avg != 5.0 {
		t.Errorf("Expected average 5.0, got %f", avg)
	}
}

func TestWindowState_ToFromJSON(t *testing.T) {
	window := &Window{
		Start: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		End:   time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
	}

	original := &WindowState{
		Key:       "test-key",
		Window:    window,
		Count:     5,
		Sum:       25.5,
		Min:       1.0,
		Max:       10.0,
		Events:    make([]*Event, 0),
		Metadata:  map[string]interface{}{"test": "value"},
		UpdatedAt: time.Now().Truncate(time.Second), // Truncate for comparison
	}

	// Convert to JSON
	jsonData, err := original.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	// Convert back from JSON
	restored := &WindowState{}
	err = restored.FromJSON(jsonData)
	if err != nil {
		t.Fatalf("FromJSON failed: %v", err)
	}

	// Verify all fields
	if restored.Key != original.Key {
		t.Errorf("Key mismatch: expected %s, got %s", original.Key, restored.Key)
	}
	if restored.Count != original.Count {
		t.Errorf("Count mismatch: expected %d, got %d", original.Count, restored.Count)
	}
	if restored.Sum != original.Sum {
		t.Errorf("Sum mismatch: expected %f, got %f", original.Sum, restored.Sum)
	}
	if restored.Min != original.Min {
		t.Errorf("Min mismatch: expected %f, got %f", original.Min, restored.Min)
	}
	if restored.Max != original.Max {
		t.Errorf("Max mismatch: expected %f, got %f", original.Max, restored.Max)
	}
}

func TestPebbleStateStore_Integration(t *testing.T) {
	// Skip if we can't create a temporary directory
	tempDir, err := os.MkdirTemp("", "pebble-test")
	if err != nil {
		t.Skipf("Cannot create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create Pebble state store
	store, err := NewPebbleStateStore(tempDir)
	if err != nil {
		t.Fatalf("Failed to create Pebble state store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Test basic operations
	testKey := "test-key"
	testValue := []byte("test-value")

	// Put and get
	err = store.Put(ctx, testKey, testValue)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	value, err := store.Get(ctx, testKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != string(testValue) {
		t.Errorf("Expected value %s, got %s", string(testValue), string(value))
	}

	// Delete
	err = store.Delete(ctx, testKey)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	value, err = store.Get(ctx, testKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if value != nil {
		t.Error("Value should be nil after delete")
	}

	// Test flush
	err = store.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}