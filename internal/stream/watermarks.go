package stream

import (
	"context"
	"sync"
	"time"
)

// watermarkManagerImpl implements WatermarkManager interface
type watermarkManagerImpl struct {
	watermarks        map[string]*Watermark // per-source watermarks
	globalWatermark   *Watermark
	maxOutOfOrderness time.Duration
	idleSourceTimeout time.Duration
	watermarkInterval time.Duration
	mu                sync.RWMutex
	running           bool
	stopCh            chan struct{}
}

// NewWatermarkManager creates a new watermark manager
func NewWatermarkManager(config *WatermarkConfig) WatermarkManager {
	if config == nil {
		config = &WatermarkConfig{
			MaxOutOfOrderness: 5 * time.Second,
			IdleSourceTimeout: 30 * time.Second,
			WatermarkInterval: 1 * time.Second,
		}
	}
	
	return &watermarkManagerImpl{
		watermarks:        make(map[string]*Watermark),
		maxOutOfOrderness: config.MaxOutOfOrderness,
		idleSourceTimeout: config.IdleSourceTimeout,
		watermarkInterval: config.WatermarkInterval,
		stopCh:            make(chan struct{}),
	}
}

// UpdateWatermark updates the watermark for a source
func (wm *watermarkManagerImpl) UpdateWatermark(source string, timestamp time.Time) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	
	// Create or update watermark for this source
	// Watermark is timestamp minus max out-of-orderness
	watermarkTime := timestamp.Add(-wm.maxOutOfOrderness)
	
	if existing, exists := wm.watermarks[source]; exists {
		// Only advance watermark (never go backwards)
		if watermarkTime.After(existing.Timestamp) {
			existing.Timestamp = watermarkTime
		}
	} else {
		wm.watermarks[source] = &Watermark{
			Timestamp: watermarkTime,
			Source:    source,
		}
	}
	
	// Update global watermark (minimum of all source watermarks)
	wm.updateGlobalWatermark()
}

// GetWatermark returns the watermark for a specific source
func (wm *watermarkManagerImpl) GetWatermark(source string) *Watermark {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	
	if watermark, exists := wm.watermarks[source]; exists {
		return &Watermark{
			Timestamp: watermark.Timestamp,
			Source:    watermark.Source,
		}
	}
	
	return nil
}

// GetGlobalWatermark returns the global watermark (minimum of all sources)
func (wm *watermarkManagerImpl) GetGlobalWatermark() *Watermark {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	
	if wm.globalWatermark != nil {
		return &Watermark{
			Timestamp: wm.globalWatermark.Timestamp,
			Source:    wm.globalWatermark.Source,
		}
	}
	
	return nil
}

// IsLateEvent checks if an event is late based on watermarks
func (wm *watermarkManagerImpl) IsLateEvent(event *Event, allowedLateness time.Duration) bool {
	globalWatermark := wm.GetGlobalWatermark()
	if globalWatermark == nil {
		return false // No watermark established yet
	}
	
	// Event is late if its timestamp is before the watermark minus allowed lateness
	latenessThreshold := globalWatermark.Timestamp.Add(-allowedLateness)
	return event.Timestamp.Before(latenessThreshold)
}

// HandleLateEvent handles a late-arriving event
func (wm *watermarkManagerImpl) HandleLateEvent(event *Event) error {
	// For now, just log the late event
	// In a more sophisticated implementation, this could:
	// - Store late events in a special buffer
	// - Update previously computed results
	// - Send late events to a dead letter queue
	return nil
}

// updateGlobalWatermark updates the global watermark (must hold lock)
func (wm *watermarkManagerImpl) updateGlobalWatermark() {
	if len(wm.watermarks) == 0 {
		wm.globalWatermark = nil
		return
	}
	
	// Find the minimum watermark across all sources
	var minWatermark *Watermark
	for _, watermark := range wm.watermarks {
		if minWatermark == nil || watermark.Timestamp.Before(minWatermark.Timestamp) {
			minWatermark = watermark
		}
	}
	
	wm.globalWatermark = &Watermark{
		Timestamp: minWatermark.Timestamp,
		Source:    "global",
	}
}

// Start starts the watermark manager
func (wm *watermarkManagerImpl) Start(ctx context.Context) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	
	if wm.running {
		return nil
	}
	
	wm.running = true
	wm.stopCh = make(chan struct{})
	
	// Start periodic watermark advancement
	go wm.watermarkLoop(ctx)
	
	// Start idle source detection
	go wm.idleSourceLoop(ctx)
	
	return nil
}

// Stop stops the watermark manager
func (wm *watermarkManagerImpl) Stop() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	
	if !wm.running {
		return nil
	}
	
	wm.running = false
	close(wm.stopCh)
	
	return nil
}

// watermarkLoop periodically advances watermarks
func (wm *watermarkManagerImpl) watermarkLoop(ctx context.Context) {
	ticker := time.NewTicker(wm.watermarkInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-wm.stopCh:
			return
		case <-ticker.C:
			// Advance watermarks based on current time
			currentTime := time.Now()
			
			wm.mu.Lock()
			for _, watermark := range wm.watermarks {
				// Advance watermark if no events received recently
				newWatermarkTime := currentTime.Add(-wm.maxOutOfOrderness)
				if newWatermarkTime.After(watermark.Timestamp) {
					watermark.Timestamp = newWatermarkTime
				}
			}
			wm.updateGlobalWatermark()
			wm.mu.Unlock()
		}
	}
}

// idleSourceLoop detects and handles idle sources
func (wm *watermarkManagerImpl) idleSourceLoop(ctx context.Context) {
	ticker := time.NewTicker(wm.idleSourceTimeout)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-wm.stopCh:
			return
		case <-ticker.C:
			wm.handleIdleSources()
		}
	}
}

// handleIdleSources removes or advances watermarks for idle sources
func (wm *watermarkManagerImpl) handleIdleSources() {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	
	currentTime := time.Now()
	for _, watermark := range wm.watermarks {
		// If watermark hasn't been updated recently, consider source idle
		if currentTime.Sub(watermark.Timestamp) > wm.idleSourceTimeout {
			// Advance watermark for idle source
			watermark.Timestamp = currentTime.Add(-wm.maxOutOfOrderness)
		}
	}
	
	wm.updateGlobalWatermark()
}

// EventTimeExtractor extracts event time from events
type EventTimeExtractor struct {
	timeExtractor TimeExtractorFunc
	fallbackToProcessingTime bool
}

// NewEventTimeExtractor creates a new event time extractor
func NewEventTimeExtractor(timeExtractor TimeExtractorFunc) *EventTimeExtractor {
	return &EventTimeExtractor{
		timeExtractor: timeExtractor,
		fallbackToProcessingTime: true,
	}
}

// ExtractEventTime extracts event time from an event
func (ete *EventTimeExtractor) ExtractEventTime(event *Event) time.Time {
	if ete.timeExtractor != nil {
		return ete.timeExtractor(event)
	}
	
	// Default: use event's timestamp field
	if !event.Timestamp.IsZero() {
		return event.Timestamp
	}
	
	// Fallback to processing time
	if ete.fallbackToProcessingTime {
		return time.Now()
	}
	
	return event.Timestamp
}

// WatermarkGenerator generates watermarks from events
type WatermarkGenerator struct {
	eventTimeExtractor *EventTimeExtractor
	watermarkManager   WatermarkManager
}

// NewWatermarkGenerator creates a new watermark generator
func NewWatermarkGenerator(eventTimeExtractor *EventTimeExtractor, watermarkManager WatermarkManager) *WatermarkGenerator {
	return &WatermarkGenerator{
		eventTimeExtractor: eventTimeExtractor,
		watermarkManager:   watermarkManager,
	}
}

// ProcessEvent processes an event and updates watermarks
func (wg *WatermarkGenerator) ProcessEvent(event *Event, source string) *Watermark {
	eventTime := wg.eventTimeExtractor.ExtractEventTime(event)
	wg.watermarkManager.UpdateWatermark(source, eventTime)
	return wg.watermarkManager.GetWatermark(source)
}

// GetGlobalWatermark returns the current global watermark
func (wg *WatermarkGenerator) GetGlobalWatermark() *Watermark {
	return wg.watermarkManager.GetGlobalWatermark()
}

// LateEventHandler handles late-arriving events
type LateEventHandler struct {
	watermarkManager WatermarkManager
	lateEventBuffer  map[string][]*Event // buffer late events by key
	maxLateEvents    int
	mu               sync.RWMutex
}

// NewLateEventHandler creates a new late event handler
func NewLateEventHandler(watermarkManager WatermarkManager, maxLateEvents int) *LateEventHandler {
	return &LateEventHandler{
		watermarkManager: watermarkManager,
		lateEventBuffer:  make(map[string][]*Event),
		maxLateEvents:    maxLateEvents,
	}
}

// HandleEvent processes an event and determines if it's late
func (leh *LateEventHandler) HandleEvent(event *Event, allowedLateness time.Duration) (bool, error) {
	isLate := leh.watermarkManager.IsLateEvent(event, allowedLateness)
	
	if isLate {
		return leh.handleLateEvent(event)
	}
	
	return false, nil
}

// handleLateEvent handles a late event
func (leh *LateEventHandler) handleLateEvent(event *Event) (bool, error) {
	leh.mu.Lock()
	defer leh.mu.Unlock()
	
	// Buffer the late event
	key := event.Key
	leh.lateEventBuffer[key] = append(leh.lateEventBuffer[key], event)
	
	// Limit buffer size
	if len(leh.lateEventBuffer[key]) > leh.maxLateEvents {
		// Remove oldest event
		leh.lateEventBuffer[key] = leh.lateEventBuffer[key][1:]
	}
	
	return true, nil
}

// GetLateEvents returns buffered late events for a key
func (leh *LateEventHandler) GetLateEvents(key string) []*Event {
	leh.mu.RLock()
	defer leh.mu.RUnlock()
	
	if events, exists := leh.lateEventBuffer[key]; exists {
		// Return a copy
		result := make([]*Event, len(events))
		copy(result, events)
		return result
	}
	
	return nil
}

// ClearLateEvents removes late events for a key
func (leh *LateEventHandler) ClearLateEvents(key string) {
	leh.mu.Lock()
	defer leh.mu.Unlock()
	
	delete(leh.lateEventBuffer, key)
}