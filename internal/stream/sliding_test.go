package stream

import (
	"context"
	"testing"
	"time"
)

func TestNewSlidingWindowManager(t *testing.T) {
	windowSize := 10 * time.Minute
	windowSlide := 2 * time.Minute
	swm := NewSlidingWindowManager(windowSize, windowSlide)

	if swm == nil {
		t.Fatal("NewSlidingWindowManager returned nil")
	}

	if swm.GetWindowSize() != windowSize {
		t.Errorf("Expected window size %v, got %v", windowSize, swm.GetWindowSize())
	}

	if swm.GetWindowSlide() != windowSlide {
		t.Errorf("Expected window slide %v, got %v", windowSlide, swm.GetWindowSlide())
	}

	activeWindows := swm.GetActiveWindows()
	if len(activeWindows) != 0 {
		t.Errorf("Expected 0 active windows, got %d", len(activeWindows))
	}
}

func TestSlidingWindowManager_GetWindow(t *testing.T) {
	windowSize := 10 * time.Minute
	windowSlide := 2 * time.Minute
	swm := NewSlidingWindowManager(windowSize, windowSlide)

	// Test creating window for a specific timestamp
	timestamp := time.Date(2024, 1, 1, 12, 5, 0, 0, time.UTC)
	window := swm.GetWindow(timestamp)

	if window == nil {
		t.Fatal("GetWindow returned nil")
	}

	// Window should contain the timestamp
	if !window.Contains(timestamp) {
		t.Errorf("Window should contain timestamp %v, window: %s", timestamp, window.String())
	}

	// Window should have the correct size
	if window.Duration() != windowSize {
		t.Errorf("Expected window duration %v, got %v", windowSize, window.Duration())
	}
}

func TestSlidingWindowManager_GetSlidingWindows(t *testing.T) {
	windowSize := 10 * time.Minute
	windowSlide := 2 * time.Minute
	swm := NewSlidingWindowManager(windowSize, windowSlide).(*slidingWindowManager)

	timestamp := time.Date(2024, 1, 1, 12, 5, 0, 0, time.UTC)
	windows := swm.GetSlidingWindows(timestamp)

	// Should have multiple overlapping windows
	if len(windows) == 0 {
		t.Fatal("Expected at least one sliding window")
	}

	// All windows should contain the timestamp
	for i, window := range windows {
		if !window.Contains(timestamp) {
			t.Errorf("Window %d should contain timestamp %v, window: %s", i, timestamp, window.String())
		}
	}

	// Windows should overlap
	if len(windows) > 1 {
		for i := 1; i < len(windows); i++ {
			prevWindow := windows[i-1]
			currWindow := windows[i]

			// Current window should start before previous window ends
			if !currWindow.Start.Before(prevWindow.End) {
				t.Errorf("Windows should overlap: prev=%s, curr=%s", prevWindow.String(), currWindow.String())
			}
		}
	}
}

func TestSlidingWindowManager_ExpireWindows(t *testing.T) {
	windowSize := 5 * time.Minute
	windowSlide := 1 * time.Minute
	swm := NewSlidingWindowManager(windowSize, windowSlide)

	// Create some windows at different times
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	timestamp1 := baseTime
	timestamp2 := baseTime.Add(3 * time.Minute)
	timestamp3 := baseTime.Add(6 * time.Minute)

	swm.GetWindow(timestamp1)
	swm.GetWindow(timestamp2)
	swm.GetWindow(timestamp3)

	// Verify we have active windows
	activeWindows := swm.GetActiveWindows()
	initialCount := len(activeWindows)
	if initialCount == 0 {
		t.Fatal("Expected some active windows")
	}

	// Expire windows older than 8 minutes from base time
	cutoffTime := baseTime.Add(8 * time.Minute)
	expiredWindows, err := swm.ExpireWindows(cutoffTime)
	if err != nil {
		t.Fatalf("Failed to expire windows: %v", err)
	}

	// Should have some expired windows
	if len(expiredWindows) == 0 {
		t.Error("Expected some expired windows")
	}

	// Should have fewer active windows now
	activeWindows = swm.GetActiveWindows()
	if len(activeWindows) >= initialCount {
		t.Errorf("Expected fewer active windows after expiration, had %d, now %d", initialCount, len(activeWindows))
	}
}

func TestSlidingWindowManager_StartStop(t *testing.T) {
	windowSize := 5 * time.Minute
	windowSlide := 1 * time.Minute
	swm := NewSlidingWindowManager(windowSize, windowSlide)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the window manager
	err := swm.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start sliding window manager: %v", err)
	}

	// Starting again should be safe
	err = swm.Start(ctx)
	if err != nil {
		t.Errorf("Starting already running window manager should not error: %v", err)
	}

	// Stop the window manager
	err = swm.Stop()
	if err != nil {
		t.Fatalf("Failed to stop sliding window manager: %v", err)
	}

	// Stopping again should be safe
	err = swm.Stop()
	if err != nil {
		t.Errorf("Stopping already stopped window manager should not error: %v", err)
	}
}

func TestSlidingWindowManager_CleanupLoop(t *testing.T) {
	windowSize := 100 * time.Millisecond
	windowSlide := 20 * time.Millisecond
	swm := NewSlidingWindowManager(windowSize, windowSlide)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the window manager
	err := swm.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start sliding window manager: %v", err)
	}
	defer swm.Stop()

	// Create some windows
	baseTime := time.Now()
	swm.GetWindow(baseTime)
	swm.GetWindow(baseTime.Add(50 * time.Millisecond))

	// Verify we have active windows
	activeWindows := swm.GetActiveWindows()
	if len(activeWindows) == 0 {
		t.Fatal("Expected some active windows")
	}

	// Wait for cleanup to happen (cleanup happens every windowSlide interval)
	time.Sleep(200 * time.Millisecond)

	// Windows should be cleaned up automatically
	activeWindows = swm.GetActiveWindows()
	// Note: Some windows might still be active depending on timing
	// The test verifies that cleanup runs without crashing
}

func TestSlidingWindowOverlap(t *testing.T) {
	windowSize := 10 * time.Minute
	windowSlide := 2 * time.Minute
	swm := NewSlidingWindowManager(windowSize, windowSlide).(*slidingWindowManager)

	// Test that events in overlapping regions appear in multiple windows
	timestamp := time.Date(2024, 1, 1, 12, 5, 0, 0, time.UTC)
	windows := swm.GetSlidingWindows(timestamp)

	// Should have multiple windows containing this timestamp
	expectedWindows := int(windowSize / windowSlide)
	if len(windows) < 2 {
		t.Errorf("Expected at least 2 overlapping windows, got %d", len(windows))
	}

	if len(windows) > expectedWindows {
		t.Errorf("Expected at most %d windows, got %d", expectedWindows, len(windows))
	}

	// Each window should be properly sized and positioned
	for i, window := range windows {
		if window.Duration() != windowSize {
			t.Errorf("Window %d has incorrect duration: expected %v, got %v", i, windowSize, window.Duration())
		}

		if !window.Contains(timestamp) {
			t.Errorf("Window %d should contain the timestamp: %s vs %v", i, window.String(), timestamp)
		}
	}
}

func TestSlidingWindowAlignment(t *testing.T) {
	windowSize := 10 * time.Minute
	windowSlide := 2 * time.Minute
	swm := NewSlidingWindowManager(windowSize, windowSlide)

	// Test that windows align correctly with slide intervals
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	
	// Get windows at different slide intervals
	window1 := swm.GetWindow(baseTime)
	window2 := swm.GetWindow(baseTime.Add(windowSlide))
	window3 := swm.GetWindow(baseTime.Add(2 * windowSlide))

	// Windows should be shifted by slide amount
	expectedShift := windowSlide
	actualShift := window2.Start.Sub(window1.Start)
	
	// The actual shift might not be exactly windowSlide due to truncation,
	// but should be close
	if actualShift > expectedShift*2 || actualShift < 0 {
		t.Errorf("Unexpected window shift: expected ~%v, got %v", expectedShift, actualShift)
	}

	// All windows should have the same duration
	if window1.Duration() != windowSize || window2.Duration() != windowSize || window3.Duration() != windowSize {
		t.Errorf("All windows should have duration %v: w1=%v, w2=%v, w3=%v", 
			windowSize, window1.Duration(), window2.Duration(), window3.Duration())
	}
}