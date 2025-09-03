package stream

import (
	"context"
	"sync"
	"time"
)

// SessionWindowState represents a session-based window that grows dynamically
type SessionWindowState struct {
	*Window
	SessionKey    string    `json:"session_key"`
	LastActivity  time.Time `json:"last_activity"`
	SessionTimeout time.Duration `json:"session_timeout"`
	EventCount    int64     `json:"event_count"`
}

// IsActive checks if the session window is still active based on timeout
func (sw *SessionWindowState) IsActive(currentTime time.Time) bool {
	return currentTime.Sub(sw.LastActivity) < sw.SessionTimeout
}

// UpdateActivity updates the session window with new activity
func (sw *SessionWindowState) UpdateActivity(timestamp time.Time) {
	if timestamp.After(sw.LastActivity) {
		sw.LastActivity = timestamp
		// Extend the session end time
		sw.End = timestamp.Add(sw.SessionTimeout)
	}
	sw.EventCount++
}

// sessionWindowManager manages session-based windows
type sessionWindowManager struct {
	sessionTimeout time.Duration
	sessionWindows map[string]*SessionWindowState // keyed by session key
	keyExtractor   func(*Event) string       // function to extract session key from event
	mu             sync.RWMutex
	running        bool
	stopCh         chan struct{}
}

// NewSessionWindowManager creates a new session window manager
func NewSessionWindowManager(sessionTimeout time.Duration, keyExtractor func(*Event) string) *sessionWindowManager {
	if keyExtractor == nil {
		// Default key extractor uses event key
		keyExtractor = func(e *Event) string { return e.Key }
	}
	
	return &sessionWindowManager{
		sessionTimeout: sessionTimeout,
		sessionWindows: make(map[string]*SessionWindowState),
		keyExtractor:   keyExtractor,
		stopCh:         make(chan struct{}),
	}
}

// GetOrCreateSessionWindow gets existing session window or creates a new one
func (swm *sessionWindowManager) GetOrCreateSessionWindow(event *Event) *SessionWindowState {
	swm.mu.Lock()
	defer swm.mu.Unlock()
	
	sessionKey := swm.keyExtractor(event)
	
	// Check if we have an existing active session
	if session, exists := swm.sessionWindows[sessionKey]; exists {
		if session.IsActive(event.Timestamp) {
			// Update the existing session
			session.UpdateActivity(event.Timestamp)
			return session
		}
		// Session has expired, remove it
		delete(swm.sessionWindows, sessionKey)
	}
	
	// Create new session window
	sessionWindow := &SessionWindowState{
		Window: &Window{
			Start: event.Timestamp,
			End:   event.Timestamp.Add(swm.sessionTimeout),
		},
		SessionKey:     sessionKey,
		LastActivity:   event.Timestamp,
		SessionTimeout: swm.sessionTimeout,
		EventCount:     1,
	}
	
	swm.sessionWindows[sessionKey] = sessionWindow
	return sessionWindow
}

// GetWindow returns the session window for the given timestamp (implements WindowManager interface)
func (swm *sessionWindowManager) GetWindow(timestamp time.Time) *Window {
	// For session windows, we need an event to determine the session key
	// This method is less useful for session windows, but we provide a basic implementation
	swm.mu.RLock()
	defer swm.mu.RUnlock()
	
	// Find any session window that contains this timestamp
	for _, session := range swm.sessionWindows {
		if session.Contains(timestamp) {
			return session.Window
		}
	}
	
	// Return a generic window if no session found
	return &Window{
		Start: timestamp,
		End:   timestamp.Add(swm.sessionTimeout),
	}
}

// GetActiveWindows returns all currently active session windows
func (swm *sessionWindowManager) GetActiveWindows() []*Window {
	swm.mu.RLock()
	defer swm.mu.RUnlock()
	
	windows := make([]*Window, 0, len(swm.sessionWindows))
	currentTime := time.Now()
	
	for _, session := range swm.sessionWindows {
		if session.IsActive(currentTime) {
			windows = append(windows, session.Window)
		}
	}
	
	return windows
}

// GetActiveSessionWindows returns all currently active session windows with full session info
func (swm *sessionWindowManager) GetActiveSessionWindows() []*SessionWindowState {
	swm.mu.RLock()
	defer swm.mu.RUnlock()
	
	sessions := make([]*SessionWindowState, 0, len(swm.sessionWindows))
	currentTime := time.Now()
	
	for _, session := range swm.sessionWindows {
		if session.IsActive(currentTime) {
			sessions = append(sessions, session)
		}
	}
	
	return sessions
}

// ExpireWindows removes session windows that have timed out
func (swm *sessionWindowManager) ExpireWindows(cutoffTime time.Time) ([]*Window, error) {
	swm.mu.Lock()
	defer swm.mu.Unlock()
	
	var expiredWindows []*Window
	for sessionKey, session := range swm.sessionWindows {
		if !session.IsActive(cutoffTime) {
			expiredWindows = append(expiredWindows, session.Window)
			delete(swm.sessionWindows, sessionKey)
		}
	}
	
	return expiredWindows, nil
}

// GetExpiredSessions returns and removes expired session windows
func (swm *sessionWindowManager) GetExpiredSessions(cutoffTime time.Time) []*SessionWindowState {
	swm.mu.Lock()
	defer swm.mu.Unlock()
	
	var expiredSessions []*SessionWindowState
	for sessionKey, session := range swm.sessionWindows {
		if !session.IsActive(cutoffTime) {
			expiredSessions = append(expiredSessions, session)
			delete(swm.sessionWindows, sessionKey)
		}
	}
	
	return expiredSessions
}

// GetWindowSize returns the session timeout (not exactly a window size)
func (swm *sessionWindowManager) GetWindowSize() time.Duration {
	return swm.sessionTimeout
}

// GetWindowSlide returns the session timeout (session windows don't slide)
func (swm *sessionWindowManager) GetWindowSlide() time.Duration {
	return swm.sessionTimeout
}

// Start starts the session window manager
func (swm *sessionWindowManager) Start(ctx context.Context) error {
	swm.mu.Lock()
	defer swm.mu.Unlock()
	
	if swm.running {
		return nil
	}
	
	swm.running = true
	swm.stopCh = make(chan struct{})
	
	// Start background cleanup goroutine
	go swm.cleanupLoop(ctx)
	
	return nil
}

// Stop stops the session window manager
func (swm *sessionWindowManager) Stop() error {
	swm.mu.Lock()
	defer swm.mu.Unlock()
	
	if !swm.running {
		return nil
	}
	
	swm.running = false
	close(swm.stopCh)
	
	return nil
}

// cleanupLoop periodically cleans up expired sessions
func (swm *sessionWindowManager) cleanupLoop(ctx context.Context) {
	// Cleanup every half of session timeout
	ticker := time.NewTicker(swm.sessionTimeout / 2)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-swm.stopCh:
			return
		case <-ticker.C:
			currentTime := time.Now()
			_, _ = swm.ExpireWindows(currentTime)
		}
	}
}

// GetSessionCount returns the number of active sessions
func (swm *sessionWindowManager) GetSessionCount() int {
	swm.mu.RLock()
	defer swm.mu.RUnlock()
	
	return len(swm.sessionWindows)
}

// GetSession returns a specific session window by key
func (swm *sessionWindowManager) GetSession(sessionKey string) (*SessionWindowState, bool) {
	swm.mu.RLock()
	defer swm.mu.RUnlock()
	
	session, exists := swm.sessionWindows[sessionKey]
	return session, exists
}