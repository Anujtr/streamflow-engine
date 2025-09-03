package stream

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/storage"
)

// windowManagerImpl implements the WindowManager interface
type windowManagerImpl struct {
	windowSize      time.Duration
	windowSlide     time.Duration // For sliding windows (future enhancement)
	activeWindows   map[string]*Window
	mu              sync.RWMutex
	ticker          *time.Ticker
	stopChan        chan struct{}
	isRunning       bool
}

// NewWindowManager creates a new window manager
func NewWindowManager(windowSize time.Duration) *windowManagerImpl {
	return &windowManagerImpl{
		windowSize:    windowSize,
		windowSlide:   windowSize, // Tumbling windows by default
		activeWindows: make(map[string]*Window),
		stopChan:      make(chan struct{}),
	}
}

// GetWindow returns the window for a given timestamp
func (wm *windowManagerImpl) GetWindow(timestamp time.Time) *Window {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	
	// Calculate window boundaries for tumbling windows
	windowStart := timestamp.Truncate(wm.windowSize)
	windowEnd := windowStart.Add(wm.windowSize)
	
	windowKey := windowStart.Format(time.RFC3339)
	
	// Check if window already exists
	if window, exists := wm.activeWindows[windowKey]; exists {
		return window
	}
	
	// Create new window
	window := &Window{
		Start: windowStart,
		End:   windowEnd,
	}
	
	wm.activeWindows[windowKey] = window
	return window
}

// GetActiveWindows returns all currently active windows
func (wm *windowManagerImpl) GetActiveWindows() []*Window {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	
	windows := make([]*Window, 0, len(wm.activeWindows))
	for _, window := range wm.activeWindows {
		windows = append(windows, window)
	}
	
	return windows
}

// ExpireWindows removes windows that are older than the cutoff time
func (wm *windowManagerImpl) ExpireWindows(cutoffTime time.Time) ([]*Window, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	
	var expiredWindows []*Window
	for key, window := range wm.activeWindows {
		if window.End.Before(cutoffTime) {
			expiredWindows = append(expiredWindows, window)
			delete(wm.activeWindows, key)
		}
	}
	
	return expiredWindows, nil
}

// GetWindowSize returns the window size
func (wm *windowManagerImpl) GetWindowSize() time.Duration {
	return wm.windowSize
}

// GetWindowSlide returns the window slide duration
func (wm *windowManagerImpl) GetWindowSlide() time.Duration {
	return wm.windowSlide
}

// Start starts the window manager
func (wm *windowManagerImpl) Start(ctx context.Context) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	
	if wm.isRunning {
		return nil
	}
	
	wm.isRunning = true
	wm.ticker = time.NewTicker(wm.windowSize / 4) // Check for expired windows frequently
	
	go wm.windowExpirationLoop(ctx)
	
	return nil
}

// Stop stops the window manager
func (wm *windowManagerImpl) Stop() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	
	if !wm.isRunning {
		return nil
	}
	
	wm.isRunning = false
	close(wm.stopChan)
	
	if wm.ticker != nil {
		wm.ticker.Stop()
	}
	
	return nil
}

// windowExpirationLoop runs in a goroutine to expire old windows
func (wm *windowManagerImpl) windowExpirationLoop(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WindowManager] Panic in expiration loop: %v", r)
		}
	}()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-wm.stopChan:
			return
		case <-wm.ticker.C:
			// Expire windows that are older than 2 * windowSize
			cutoffTime := time.Now().Add(-2 * wm.windowSize)
			expiredWindows, err := wm.ExpireWindows(cutoffTime)
			if err != nil {
				log.Printf("[WindowManager] Error expiring windows: %v", err)
			} else if len(expiredWindows) > 0 {
				log.Printf("[WindowManager] Expired %d windows", len(expiredWindows))
			}
		}
	}
}

// windowedStreamImpl implements the WindowedStream interface
type windowedStreamImpl struct {
	baseStream    *streamImpl
	windowManager WindowManager
	windowSize    time.Duration
	metrics       *ProcessorMetrics
	mu            sync.RWMutex
}

// NewWindowedStream creates a new windowed stream
func NewWindowedStream(baseStream *streamImpl, windowSize time.Duration) *windowedStreamImpl {
	windowManager := NewWindowManager(windowSize)
	
	return &windowedStreamImpl{
		baseStream:    baseStream,
		windowManager: windowManager,
		windowSize:    windowSize,
		metrics:       &ProcessorMetrics{},
	}
}

// GroupBy creates a windowed grouped stream
func (ws *windowedStreamImpl) GroupBy(keyExtractor KeyExtractorFunc) WindowedGroupedStream {
	return NewWindowedGroupedStreamFromWindowed(ws, keyExtractor)
}

// Count creates an aggregated stream that counts events in each window
func (ws *windowedStreamImpl) Count() AggregatedStream {
	return NewAggregatedStream(ws, "count", nil)
}

// Sum creates an aggregated stream that sums values in each window
func (ws *windowedStreamImpl) Sum(valueExtractor ValueExtractorFunc) AggregatedStream {
	return NewAggregatedStream(ws, "sum", valueExtractor)
}

// Average creates an aggregated stream that averages values in each window
func (ws *windowedStreamImpl) Average(valueExtractor ValueExtractorFunc) AggregatedStream {
	return NewAggregatedStream(ws, "average", valueExtractor)
}

// Min creates an aggregated stream that finds minimum values in each window
func (ws *windowedStreamImpl) Min(valueExtractor ValueExtractorFunc) AggregatedStream {
	return NewAggregatedStream(ws, "min", valueExtractor)
}

// Max creates an aggregated stream that finds maximum values in each window
func (ws *windowedStreamImpl) Max(valueExtractor ValueExtractorFunc) AggregatedStream {
	return NewAggregatedStream(ws, "max", valueExtractor)
}

// Reduce creates an aggregated stream with custom aggregation
func (ws *windowedStreamImpl) Reduce(aggregator AggregatorFunc) AggregatedStream {
	return NewAggregatedStreamWithCustomAggregator(ws, aggregator)
}

// ForEach processes each windowed event with the provided function
func (ws *windowedStreamImpl) ForEach(processor func(*WindowedEvent)) error {
	// Start window manager
	ctx := context.Background()
	if err := ws.windowManager.Start(ctx); err != nil {
		return err
	}
	defer ws.windowManager.Stop()
	
	return ws.baseStream.processor.processStream(ws.baseStream.inputTopic, func(event *Event) error {
		// Apply base stream operations first
		processedEvent, shouldProcess := ws.baseStream.applyOperations(event)
		if shouldProcess && processedEvent != nil {
			// Get the window for this event
			window := ws.windowManager.GetWindow(processedEvent.Timestamp)
			
			// Create windowed event
			windowedEvent := &WindowedEvent{
				Event:  processedEvent,
				Window: window,
			}
			
			processor(windowedEvent)
			atomic.AddInt64(&ws.metrics.EventsProcessed, 1)
		}
		return nil
	})
}

// Output sends windowed events to the specified topic
func (ws *windowedStreamImpl) Output(topic string) error {
	// Start window manager
	ctx := context.Background()
	if err := ws.windowManager.Start(ctx); err != nil {
		return err
	}
	defer ws.windowManager.Stop()
	
	return ws.baseStream.processor.processStream(ws.baseStream.inputTopic, func(event *Event) error {
		// Apply base stream operations first
		processedEvent, shouldProcess := ws.baseStream.applyOperations(event)
		if shouldProcess && processedEvent != nil {
			// Get the window for this event
			window := ws.windowManager.GetWindow(processedEvent.Timestamp)
			
			// Create windowed event and convert to output format
			windowedEvent := &WindowedEvent{
				Event:  processedEvent,
				Window: window,
			}
			
			// Add window information to event metadata
			if windowedEvent.Event.Metadata == nil {
				windowedEvent.Event.Metadata = make(map[string]interface{})
			}
			windowedEvent.Event.Metadata["window_start"] = window.Start
			windowedEvent.Event.Metadata["window_end"] = window.End
			
			// Convert back to storage.Message and produce to output topic
			outputMsg := &storage.Message{
				Key:   windowedEvent.Event.Key,
				Value: windowedEvent.Event.Value,
			}
			
			_, _, err := ws.baseStream.processor.storage.Produce(topic, outputMsg)
			if err != nil {
				atomic.AddInt64(&ws.metrics.ProcessingErrors, 1)
				log.Printf("[WindowedStream] Failed to produce to output topic %s: %v", topic, err)
				return err
			}
			
			atomic.AddInt64(&ws.metrics.EventsProcessed, 1)
		}
		return nil
	})
}

// Start begins processing the windowed stream
func (ws *windowedStreamImpl) Start(ctx context.Context) error {
	if err := ws.windowManager.Start(ctx); err != nil {
		return err
	}
	return ws.baseStream.Start(ctx)
}

// Stop stops windowed stream processing
func (ws *windowedStreamImpl) Stop() error {
	ws.windowManager.Stop()
	return ws.baseStream.Stop()
}

// GetMetrics returns the current processing metrics
func (ws *windowedStreamImpl) GetMetrics() *ProcessorMetrics {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	
	// Combine metrics from base stream and windowed stream
	baseMetrics := ws.baseStream.GetMetrics()
	
	return &ProcessorMetrics{
		EventsProcessed:   baseMetrics.EventsProcessed + atomic.LoadInt64(&ws.metrics.EventsProcessed),
		EventsFiltered:    baseMetrics.EventsFiltered + atomic.LoadInt64(&ws.metrics.EventsFiltered),
		EventsTransformed: baseMetrics.EventsTransformed + atomic.LoadInt64(&ws.metrics.EventsTransformed),
		WindowsCreated:    atomic.LoadInt64(&ws.metrics.WindowsCreated),
		WindowsExpired:    atomic.LoadInt64(&ws.metrics.WindowsExpired),
		ProcessingErrors:  baseMetrics.ProcessingErrors + atomic.LoadInt64(&ws.metrics.ProcessingErrors),
		AvgLatency:        ws.metrics.AvgLatency,
		ThroughputPerSec:  ws.metrics.ThroughputPerSec,
	}
}

// Phase 5 Advanced Window Constructors

// NewSlidingWindowedStream creates a sliding windowed stream
func NewSlidingWindowedStream(baseStream *streamImpl, windowSize, slideSize time.Duration) WindowedStream {
	slidingWindowManager := NewSlidingWindowManager(windowSize, slideSize)
	
	return &windowedStreamImpl{
		baseStream:    baseStream,
		windowSize:    windowSize,
		windowManager: slidingWindowManager,
		metrics:       &ProcessorMetrics{},
	}
}

// NewSessionWindowedStream creates a session windowed stream
func NewSessionWindowedStream(baseStream *streamImpl, sessionTimeout time.Duration) WindowedStream {
	sessionWindowManager := NewSessionWindowManager(sessionTimeout, func(e *Event) string {
		return e.Key // Default session key extractor
	})
	
	// Adapt session window manager to WindowManager interface
	adaptedManager := &sessionWindowAdapter{
		sessionManager: sessionWindowManager,
		sessionTimeout: sessionTimeout,
	}
	
	return &windowedStreamImpl{
		baseStream:    baseStream,
		windowSize:    sessionTimeout,
		windowManager: adaptedManager,
		metrics:       &ProcessorMetrics{},
	}
}

// sessionWindowAdapter adapts sessionWindowManager to WindowManager interface
type sessionWindowAdapter struct {
	sessionManager *sessionWindowManager
	sessionTimeout time.Duration
}

func (swa *sessionWindowAdapter) GetWindow(timestamp time.Time) *Window {
	// For session windows, we need an event to determine the session
	// This is a simplified implementation
	return &Window{
		Start: timestamp,
		End:   timestamp.Add(swa.sessionTimeout),
	}
}

func (swa *sessionWindowAdapter) GetActiveWindows() []*Window {
	return swa.sessionManager.GetActiveWindows()
}

func (swa *sessionWindowAdapter) ExpireWindows(cutoffTime time.Time) ([]*Window, error) {
	return swa.sessionManager.ExpireWindows(cutoffTime)
}

func (swa *sessionWindowAdapter) GetWindowSize() time.Duration {
	return swa.sessionTimeout
}

func (swa *sessionWindowAdapter) GetWindowSlide() time.Duration {
	return swa.sessionTimeout
}

func (swa *sessionWindowAdapter) Start(ctx context.Context) error {
	return swa.sessionManager.Start(ctx)
}

func (swa *sessionWindowAdapter) Stop() error {
	return swa.sessionManager.Stop()
}