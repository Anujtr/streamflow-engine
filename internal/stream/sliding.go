package stream

import (
	"context"
	"sync"
	"time"
)

// slidingWindowManager manages sliding time windows
type slidingWindowManager struct {
	windowSize    time.Duration
	windowSlide   time.Duration
	activeWindows map[string]*Window
	mu            sync.RWMutex
	running       bool
	stopCh        chan struct{}
}

// NewSlidingWindowManager creates a new sliding window manager
func NewSlidingWindowManager(windowSize, windowSlide time.Duration) WindowManager {
	return &slidingWindowManager{
		windowSize:    windowSize,
		windowSlide:   windowSlide,
		activeWindows: make(map[string]*Window),
		stopCh:        make(chan struct{}),
	}
}

// GetWindow returns all windows that contain the given timestamp for sliding windows
func (swm *slidingWindowManager) GetWindow(timestamp time.Time) *Window {
	swm.mu.Lock()
	defer swm.mu.Unlock()
	
	// For sliding windows, we need to find the latest window that contains this timestamp
	// Windows slide by windowSlide interval, so we calculate the window start
	windowStart := timestamp.Truncate(swm.windowSlide)
	
	// Adjust to find the correct window that contains this timestamp
	// We may need to go back several slides to find the window
	for {
		windowEnd := windowStart.Add(swm.windowSize)
		if !timestamp.Before(windowStart) && timestamp.Before(windowEnd) {
			// This window contains our timestamp
			window := &Window{
				Start: windowStart,
				End:   windowEnd,
			}
			
			windowKey := swm.windowKey(window)
			swm.activeWindows[windowKey] = window
			return window
		}
		
		// Try the previous slide
		windowStart = windowStart.Add(-swm.windowSlide)
		
		// Safety check - don't go too far back
		if windowStart.Before(timestamp.Add(-swm.windowSize)) {
			break
		}
	}
	
	// If we can't find an existing window, create one
	windowStart = timestamp.Truncate(swm.windowSlide)
	window := &Window{
		Start: windowStart,
		End:   windowStart.Add(swm.windowSize),
	}
	
	windowKey := swm.windowKey(window)
	swm.activeWindows[windowKey] = window
	return window
}

// GetActiveWindows returns all currently active windows
func (swm *slidingWindowManager) GetActiveWindows() []*Window {
	swm.mu.RLock()
	defer swm.mu.RUnlock()
	
	windows := make([]*Window, 0, len(swm.activeWindows))
	for _, window := range swm.activeWindows {
		windows = append(windows, window)
	}
	
	return windows
}

// ExpireWindows removes windows that have ended before the cutoff time
func (swm *slidingWindowManager) ExpireWindows(cutoffTime time.Time) ([]*Window, error) {
	swm.mu.Lock()
	defer swm.mu.Unlock()
	
	var expiredWindows []*Window
	for key, window := range swm.activeWindows {
		if window.End.Before(cutoffTime) {
			expiredWindows = append(expiredWindows, window)
			delete(swm.activeWindows, key)
		}
	}
	
	return expiredWindows, nil
}

// GetWindowSize returns the window size
func (swm *slidingWindowManager) GetWindowSize() time.Duration {
	return swm.windowSize
}

// GetWindowSlide returns the window slide interval
func (swm *slidingWindowManager) GetWindowSlide() time.Duration {
	return swm.windowSlide
}

// Start starts the sliding window manager
func (swm *slidingWindowManager) Start(ctx context.Context) error {
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

// Stop stops the sliding window manager
func (swm *slidingWindowManager) Stop() error {
	swm.mu.Lock()
	defer swm.mu.Unlock()
	
	if !swm.running {
		return nil
	}
	
	swm.running = false
	close(swm.stopCh)
	
	return nil
}

// windowKey generates a unique key for a window
func (swm *slidingWindowManager) windowKey(window *Window) string {
	return window.Start.Format(time.RFC3339Nano)
}

// cleanupLoop periodically cleans up expired windows
func (swm *slidingWindowManager) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(swm.windowSlide)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-swm.stopCh:
			return
		case <-ticker.C:
			// Clean up expired windows (those that ended more than windowSize ago)
			cutoffTime := time.Now().Add(-swm.windowSize)
			_, _ = swm.ExpireWindows(cutoffTime)
		}
	}
}

// GetSlidingWindows returns all sliding windows that contain the given timestamp
func (swm *slidingWindowManager) GetSlidingWindows(timestamp time.Time) []*Window {
	swm.mu.Lock()
	defer swm.mu.Unlock()
	
	var windows []*Window
	
	// Calculate how many windows can contain this timestamp
	maxWindows := int(swm.windowSize / swm.windowSlide)
	
	for i := 0; i < maxWindows; i++ {
		windowStart := timestamp.Truncate(swm.windowSlide).Add(time.Duration(-i) * swm.windowSlide)
		windowEnd := windowStart.Add(swm.windowSize)
		
		// Check if this window contains the timestamp
		if !timestamp.Before(windowStart) && timestamp.Before(windowEnd) {
			window := &Window{
				Start: windowStart,
				End:   windowEnd,
			}
			
			windowKey := swm.windowKey(window)
			swm.activeWindows[windowKey] = window
			windows = append(windows, window)
		}
	}
	
	return windows
}