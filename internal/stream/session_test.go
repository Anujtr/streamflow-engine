package stream

import (
	"context"
	"testing"
	"time"
)

func TestNewSessionWindowManager(t *testing.T) {
	sessionTimeout := 5 * time.Minute
	swm := NewSessionWindowManager(sessionTimeout, nil)

	if swm == nil {
		t.Fatal("NewSessionWindowManager returned nil")
	}

	if swm.GetWindowSize() != sessionTimeout {
		t.Errorf("Expected session timeout %v, got %v", sessionTimeout, swm.GetWindowSize())
	}

	activeWindows := swm.GetActiveWindows()
	if len(activeWindows) != 0 {
		t.Errorf("Expected 0 active windows, got %d", len(activeWindows))
	}
}

func TestSessionWindow_IsActive(t *testing.T) {
	sessionTimeout := 5 * time.Minute
	startTime := time.Now()

	session := &SessionWindowState{
		Window: &Window{
			Start: startTime,
			End:   startTime.Add(sessionTimeout),
		},
		SessionKey:     "test-key",
		LastActivity:   startTime,
		SessionTimeout: sessionTimeout,
		EventCount:     1,
	}

	// Should be active immediately
	if !session.IsActive(startTime) {
		t.Error("Session should be active at start time")
	}

	// Should be active within timeout
	if !session.IsActive(startTime.Add(2 * time.Minute)) {
		t.Error("Session should be active within timeout")
	}

	// Should not be active after timeout
	if session.IsActive(startTime.Add(sessionTimeout + time.Minute)) {
		t.Error("Session should not be active after timeout")
	}
}

func TestSessionWindow_UpdateActivity(t *testing.T) {
	sessionTimeout := 5 * time.Minute
	startTime := time.Now()

	session := &SessionWindowState{
		Window: &Window{
			Start: startTime,
			End:   startTime.Add(sessionTimeout),
		},
		SessionKey:     "test-key",
		LastActivity:   startTime,
		SessionTimeout: sessionTimeout,
		EventCount:     1,
	}

	// Update with later timestamp
	laterTime := startTime.Add(2 * time.Minute)
	session.UpdateActivity(laterTime)

	if !session.LastActivity.Equal(laterTime) {
		t.Errorf("Expected last activity %v, got %v", laterTime, session.LastActivity)
	}

	if session.EventCount != 2 {
		t.Errorf("Expected event count 2, got %d", session.EventCount)
	}

	// Session end should be extended
	expectedEnd := laterTime.Add(sessionTimeout)
	if !session.End.Equal(expectedEnd) {
		t.Errorf("Expected session end %v, got %v", expectedEnd, session.End)
	}

	// Update with earlier timestamp (should not change last activity)
	earlierTime := startTime.Add(1 * time.Minute)
	session.UpdateActivity(earlierTime)

	if !session.LastActivity.Equal(laterTime) {
		t.Errorf("Last activity should remain %v, got %v", laterTime, session.LastActivity)
	}

	if session.EventCount != 3 {
		t.Errorf("Expected event count 3, got %d", session.EventCount)
	}
}

func TestSessionWindowManager_GetOrCreateSessionWindow(t *testing.T) {
	sessionTimeout := 5 * time.Minute
	swm := NewSessionWindowManager(sessionTimeout, nil)

	event := &Event{
		Key:       "test-key",
		Value:     []byte("test-value"),
		Timestamp: time.Now(),
	}

	// First call should create new session
	session1 := swm.GetOrCreateSessionWindow(event)
	if session1 == nil {
		t.Fatal("Expected session window, got nil")
	}

	if session1.SessionKey != "test-key" {
		t.Errorf("Expected session key 'test-key', got '%s'", session1.SessionKey)
	}

	if session1.EventCount != 1 {
		t.Errorf("Expected event count 1, got %d", session1.EventCount)
	}

	// Second call with same key should return same session (updated)
	event2 := &Event{
		Key:       "test-key",
		Value:     []byte("test-value-2"),
		Timestamp: event.Timestamp.Add(1 * time.Minute),
	}

	session2 := swm.GetOrCreateSessionWindow(event2)
	if session2 != session1 {
		t.Error("Expected same session instance for same key")
	}

	if session2.EventCount != 2 {
		t.Errorf("Expected event count 2, got %d", session2.EventCount)
	}

	// Call with different key should create new session
	event3 := &Event{
		Key:       "different-key",
		Value:     []byte("test-value-3"),
		Timestamp: time.Now(),
	}

	session3 := swm.GetOrCreateSessionWindow(event3)
	if session3 == session1 {
		t.Error("Expected different session instance for different key")
	}

	if session3.SessionKey != "different-key" {
		t.Errorf("Expected session key 'different-key', got '%s'", session3.SessionKey)
	}
}

func TestSessionWindowManager_SessionExpiration(t *testing.T) {
	sessionTimeout := 100 * time.Millisecond // Short timeout for testing
	swm := NewSessionWindowManager(sessionTimeout, nil)

	event := &Event{
		Key:       "test-key",
		Value:     []byte("test-value"),
		Timestamp: time.Now(),
	}

	// Create a session
	session1 := swm.GetOrCreateSessionWindow(event)
	if session1 == nil {
		t.Fatal("Expected session window, got nil")
	}

	// Wait for session to expire
	time.Sleep(sessionTimeout + 50*time.Millisecond)

	// New event should create a new session (old one expired)
	event2 := &Event{
		Key:       "test-key",
		Value:     []byte("test-value-2"),
		Timestamp: time.Now(),
	}

	session2 := swm.GetOrCreateSessionWindow(event2)
	if session2 == session1 {
		t.Error("Expected new session instance after expiration")
	}

	if session2.EventCount != 1 {
		t.Errorf("Expected event count 1 for new session, got %d", session2.EventCount)
	}
}

func TestSessionWindowManager_GetActiveWindows(t *testing.T) {
	sessionTimeout := 5 * time.Minute
	swm := NewSessionWindowManager(sessionTimeout, nil)

	// Initially no active windows
	activeWindows := swm.GetActiveWindows()
	if len(activeWindows) != 0 {
		t.Errorf("Expected 0 active windows, got %d", len(activeWindows))
	}

	// Create some sessions
	event1 := &Event{Key: "key1", Timestamp: time.Now()}
	event2 := &Event{Key: "key2", Timestamp: time.Now()}
	event3 := &Event{Key: "key3", Timestamp: time.Now()}

	swm.GetOrCreateSessionWindow(event1)
	swm.GetOrCreateSessionWindow(event2)
	swm.GetOrCreateSessionWindow(event3)

	// Should have 3 active windows
	activeWindows = swm.GetActiveWindows()
	if len(activeWindows) != 3 {
		t.Errorf("Expected 3 active windows, got %d", len(activeWindows))
	}
}

func TestSessionWindowManager_GetActiveSessionWindows(t *testing.T) {
	sessionTimeout := 5 * time.Minute
	swm := NewSessionWindowManager(sessionTimeout, nil)

	// Create some sessions
	event1 := &Event{Key: "key1", Timestamp: time.Now()}
	event2 := &Event{Key: "key2", Timestamp: time.Now()}

	swm.GetOrCreateSessionWindow(event1)
	swm.GetOrCreateSessionWindow(event2)

	// Get active session windows
	activeSessions := swm.GetActiveSessionWindows()
	if len(activeSessions) != 2 {
		t.Errorf("Expected 2 active sessions, got %d", len(activeSessions))
	}

	// Verify session details
	for _, session := range activeSessions {
		if session.EventCount != 1 {
			t.Errorf("Expected event count 1, got %d", session.EventCount)
		}
		if session.SessionTimeout != sessionTimeout {
			t.Errorf("Expected session timeout %v, got %v", sessionTimeout, session.SessionTimeout)
		}
	}
}

func TestSessionWindowManager_ExpireWindows(t *testing.T) {
	sessionTimeout := 100 * time.Millisecond // Short timeout for testing
	swm := NewSessionWindowManager(sessionTimeout, nil)

	// Create some sessions
	now := time.Now()
	event1 := &Event{Key: "key1", Timestamp: now}
	event2 := &Event{Key: "key2", Timestamp: now.Add(50 * time.Millisecond)}

	swm.GetOrCreateSessionWindow(event1)
	swm.GetOrCreateSessionWindow(event2)

	// Initially should have 2 active windows
	activeWindows := swm.GetActiveWindows()
	if len(activeWindows) != 2 {
		t.Errorf("Expected 2 active windows, got %d", len(activeWindows))
	}

	// Wait and expire old sessions
	time.Sleep(sessionTimeout + 50*time.Millisecond)
	cutoffTime := time.Now()

	expiredWindows, err := swm.ExpireWindows(cutoffTime)
	if err != nil {
		t.Fatalf("Failed to expire windows: %v", err)
	}

	// Should have expired some windows
	if len(expiredWindows) == 0 {
		t.Error("Expected some expired windows")
	}

	// Should have fewer active windows now
	activeWindows = swm.GetActiveWindows()
	if len(activeWindows) != 0 {
		t.Errorf("Expected 0 active windows after expiration, got %d", len(activeWindows))
	}
}

func TestSessionWindowManager_StartStop(t *testing.T) {
	sessionTimeout := 5 * time.Minute
	swm := NewSessionWindowManager(sessionTimeout, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the session window manager
	err := swm.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start session window manager: %v", err)
	}

	// Starting again should be safe
	err = swm.Start(ctx)
	if err != nil {
		t.Errorf("Starting already running window manager should not error: %v", err)
	}

	// Stop the window manager
	err = swm.Stop()
	if err != nil {
		t.Fatalf("Failed to stop session window manager: %v", err)
	}

	// Stopping again should be safe
	err = swm.Stop()
	if err != nil {
		t.Errorf("Stopping already stopped window manager should not error: %v", err)
	}
}

func TestSessionWindowManager_GetSessionCount(t *testing.T) {
	sessionTimeout := 5 * time.Minute
	swm := NewSessionWindowManager(sessionTimeout, nil)

	// Initially no sessions
	if count := swm.GetSessionCount(); count != 0 {
		t.Errorf("Expected 0 sessions, got %d", count)
	}

	// Create some sessions
	event1 := &Event{Key: "key1", Timestamp: time.Now()}
	event2 := &Event{Key: "key2", Timestamp: time.Now()}

	swm.GetOrCreateSessionWindow(event1)
	if count := swm.GetSessionCount(); count != 1 {
		t.Errorf("Expected 1 session, got %d", count)
	}

	swm.GetOrCreateSessionWindow(event2)
	if count := swm.GetSessionCount(); count != 2 {
		t.Errorf("Expected 2 sessions, got %d", count)
	}

	// Same key should not increase count
	swm.GetOrCreateSessionWindow(event1)
	if count := swm.GetSessionCount(); count != 2 {
		t.Errorf("Expected 2 sessions, got %d", count)
	}
}

func TestSessionWindowManager_GetSession(t *testing.T) {
	sessionTimeout := 5 * time.Minute
	swm := NewSessionWindowManager(sessionTimeout, nil)

	// Get non-existent session
	session, exists := swm.GetSession("nonexistent")
	if exists {
		t.Error("Expected session not to exist")
	}
	if session != nil {
		t.Error("Expected nil session")
	}

	// Create a session
	event := &Event{Key: "test-key", Timestamp: time.Now()}
	createdSession := swm.GetOrCreateSessionWindow(event)

	// Get the created session
	session, exists = swm.GetSession("test-key")
	if !exists {
		t.Error("Expected session to exist")
	}
	if session != createdSession {
		t.Error("Expected same session instance")
	}
}

func TestSessionWindowManager_CustomKeyExtractor(t *testing.T) {
	sessionTimeout := 5 * time.Minute
	
	// Custom key extractor that uses metadata
	keyExtractor := func(e *Event) string {
		if e.Metadata != nil {
			if userID, exists := e.Metadata["user_id"]; exists {
				return userID.(string)
			}
		}
		return e.Key
	}
	
	swm := NewSessionWindowManager(sessionTimeout, keyExtractor)

	// Create event with user_id in metadata
	event := &Event{
		Key:       "event-key",
		Value:     []byte("test"),
		Timestamp: time.Now(),
		Metadata:  map[string]interface{}{"user_id": "user123"},
	}

	session := swm.GetOrCreateSessionWindow(event)
	if session.SessionKey != "user123" {
		t.Errorf("Expected session key 'user123', got '%s'", session.SessionKey)
	}

	// Event without metadata should use default key
	event2 := &Event{
		Key:       "event-key-2",
		Value:     []byte("test"),
		Timestamp: time.Now(),
	}

	session2 := swm.GetOrCreateSessionWindow(event2)
	if session2.SessionKey != "event-key-2" {
		t.Errorf("Expected session key 'event-key-2', got '%s'", session2.SessionKey)
	}
}