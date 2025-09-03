package stream

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/storage"
)

// groupedStreamImpl implements the GroupedStream interface
type groupedStreamImpl struct {
	baseStream    *streamImpl
	keyExtractor  KeyExtractorFunc
	groupMap      map[string][]*Event
	metrics       *ProcessorMetrics
	mu            sync.RWMutex
}

// NewGroupedStream creates a new grouped stream
func NewGroupedStream(baseStream *streamImpl, keyExtractor KeyExtractorFunc) *groupedStreamImpl {
	return &groupedStreamImpl{
		baseStream:   baseStream,
		keyExtractor: keyExtractor,
		groupMap:     make(map[string][]*Event),
		metrics:      &ProcessorMetrics{},
	}
}

// Window creates a windowed grouped stream
func (gs *groupedStreamImpl) Window(duration time.Duration) WindowedGroupedStream {
	return NewWindowedGroupedStream(gs, duration)
}

// SessionWindow creates a session windowed grouped stream
func (gs *groupedStreamImpl) SessionWindow(timeout time.Duration) WindowedGroupedStream {
	// For now, implement as tumbling window - session windows can be enhanced later
	return NewWindowedGroupedStream(gs, timeout)
}

// Count creates an aggregated stream that counts events per group
func (gs *groupedStreamImpl) Count() AggregatedStream {
	return NewGroupedAggregatedStream(gs, "count", nil)
}

// Sum creates an aggregated stream that sums values per group
func (gs *groupedStreamImpl) Sum(valueExtractor ValueExtractorFunc) AggregatedStream {
	return NewGroupedAggregatedStream(gs, "sum", valueExtractor)
}

// Average creates an aggregated stream that averages values per group
func (gs *groupedStreamImpl) Average(valueExtractor ValueExtractorFunc) AggregatedStream {
	return NewGroupedAggregatedStream(gs, "average", valueExtractor)
}

// Min creates an aggregated stream that finds minimum values per group
func (gs *groupedStreamImpl) Min(valueExtractor ValueExtractorFunc) AggregatedStream {
	return NewGroupedAggregatedStream(gs, "min", valueExtractor)
}

// Max creates an aggregated stream that finds maximum values per group
func (gs *groupedStreamImpl) Max(valueExtractor ValueExtractorFunc) AggregatedStream {
	return NewGroupedAggregatedStream(gs, "max", valueExtractor)
}

// Reduce creates an aggregated stream with custom aggregation per group
func (gs *groupedStreamImpl) Reduce(aggregator AggregatorFunc) AggregatedStream {
	return NewGroupedAggregatedStreamWithCustomAggregator(gs, aggregator)
}

// ForEach processes each group of events with the provided function
func (gs *groupedStreamImpl) ForEach(processor func(string, []*Event)) error {
	return gs.baseStream.processor.processStream(gs.baseStream.inputTopic, func(event *Event) error {
		// Apply base stream operations first
		processedEvent, shouldProcess := gs.baseStream.applyOperations(event)
		if shouldProcess && processedEvent != nil {
			// Extract grouping key
			groupKey := gs.keyExtractor(processedEvent)
			
			gs.mu.Lock()
			if _, exists := gs.groupMap[groupKey]; !exists {
				gs.groupMap[groupKey] = make([]*Event, 0)
			}
			gs.groupMap[groupKey] = append(gs.groupMap[groupKey], processedEvent.Clone())
			events := make([]*Event, len(gs.groupMap[groupKey]))
			copy(events, gs.groupMap[groupKey])
			gs.mu.Unlock()
			
			processor(groupKey, events)
			atomic.AddInt64(&gs.metrics.EventsProcessed, 1)
		}
		return nil
	})
}

// Output sends grouped events to the specified topic
func (gs *groupedStreamImpl) Output(topic string) error {
	return gs.baseStream.processor.processStream(gs.baseStream.inputTopic, func(event *Event) error {
		// Apply base stream operations first
		processedEvent, shouldProcess := gs.baseStream.applyOperations(event)
		if shouldProcess && processedEvent != nil {
			// Extract grouping key and add to metadata
			groupKey := gs.keyExtractor(processedEvent)
			
			if processedEvent.Metadata == nil {
				processedEvent.Metadata = make(map[string]interface{})
			}
			processedEvent.Metadata["group_key"] = groupKey
			
			// Convert back to storage.Message and produce to output topic
			outputMsg := &storage.Message{
				Key:   processedEvent.Key,
				Value: processedEvent.Value,
			}
			
			_, _, err := gs.baseStream.processor.storage.Produce(topic, outputMsg)
			if err != nil {
				atomic.AddInt64(&gs.metrics.ProcessingErrors, 1)
				log.Printf("[GroupedStream] Failed to produce to output topic %s: %v", topic, err)
				return err
			}
			
			atomic.AddInt64(&gs.metrics.EventsProcessed, 1)
		}
		return nil
	})
}

// Start begins processing the grouped stream
func (gs *groupedStreamImpl) Start(ctx context.Context) error {
	return gs.baseStream.Start(ctx)
}

// Stop stops grouped stream processing
func (gs *groupedStreamImpl) Stop() error {
	return gs.baseStream.Stop()
}

// GetMetrics returns the current processing metrics
func (gs *groupedStreamImpl) GetMetrics() *ProcessorMetrics {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	
	// Combine metrics from base stream and grouped stream
	baseMetrics := gs.baseStream.GetMetrics()
	
	return &ProcessorMetrics{
		EventsProcessed:   baseMetrics.EventsProcessed + atomic.LoadInt64(&gs.metrics.EventsProcessed),
		EventsFiltered:    baseMetrics.EventsFiltered + atomic.LoadInt64(&gs.metrics.EventsFiltered),
		EventsTransformed: baseMetrics.EventsTransformed + atomic.LoadInt64(&gs.metrics.EventsTransformed),
		ProcessingErrors:  baseMetrics.ProcessingErrors + atomic.LoadInt64(&gs.metrics.ProcessingErrors),
		AvgLatency:        gs.metrics.AvgLatency,
		ThroughputPerSec:  gs.metrics.ThroughputPerSec,
	}
}

// windowedGroupedStreamImpl implements the WindowedGroupedStream interface
type windowedGroupedStreamImpl struct {
	groupedStream *groupedStreamImpl
	windowManager WindowManager
	windowSize    time.Duration
	groupWindows  map[string]map[string][]*Event // groupKey -> windowKey -> events
	metrics       *ProcessorMetrics
	mu            sync.RWMutex
}

// NewWindowedGroupedStream creates a new windowed grouped stream from a grouped stream
func NewWindowedGroupedStream(groupedStream *groupedStreamImpl, windowSize time.Duration) *windowedGroupedStreamImpl {
	windowManager := NewWindowManager(windowSize)
	
	return &windowedGroupedStreamImpl{
		groupedStream: groupedStream,
		windowManager: windowManager,
		windowSize:    windowSize,
		groupWindows:  make(map[string]map[string][]*Event),
		metrics:       &ProcessorMetrics{},
	}
}

// NewWindowedGroupedStreamFromWindowed creates a new windowed grouped stream from a windowed stream
func NewWindowedGroupedStreamFromWindowed(windowedStream *windowedStreamImpl, keyExtractor KeyExtractorFunc) *windowedGroupedStreamImpl {
	// Create a grouped stream from the base stream first
	groupedStream := NewGroupedStream(windowedStream.baseStream, keyExtractor)
	
	return &windowedGroupedStreamImpl{
		groupedStream: groupedStream,
		windowManager: windowedStream.windowManager,
		windowSize:    windowedStream.windowSize,
		groupWindows:  make(map[string]map[string][]*Event),
		metrics:       &ProcessorMetrics{},
	}
}

// Count creates an aggregated stream that counts events per group and window
func (wgs *windowedGroupedStreamImpl) Count() AggregatedStream {
	return NewWindowedGroupedAggregatedStream(wgs, "count", nil)
}

// Sum creates an aggregated stream that sums values per group and window
func (wgs *windowedGroupedStreamImpl) Sum(valueExtractor ValueExtractorFunc) AggregatedStream {
	return NewWindowedGroupedAggregatedStream(wgs, "sum", valueExtractor)
}

// Average creates an aggregated stream that averages values per group and window
func (wgs *windowedGroupedStreamImpl) Average(valueExtractor ValueExtractorFunc) AggregatedStream {
	return NewWindowedGroupedAggregatedStream(wgs, "average", valueExtractor)
}

// Min creates an aggregated stream that finds minimum values per group and window
func (wgs *windowedGroupedStreamImpl) Min(valueExtractor ValueExtractorFunc) AggregatedStream {
	return NewWindowedGroupedAggregatedStream(wgs, "min", valueExtractor)
}

// Max creates an aggregated stream that finds maximum values per group and window
func (wgs *windowedGroupedStreamImpl) Max(valueExtractor ValueExtractorFunc) AggregatedStream {
	return NewWindowedGroupedAggregatedStream(wgs, "max", valueExtractor)
}

// Reduce creates an aggregated stream with custom aggregation per group and window
func (wgs *windowedGroupedStreamImpl) Reduce(aggregator AggregatorFunc) AggregatedStream {
	return NewWindowedGroupedAggregatedStreamWithCustomAggregator(wgs, aggregator)
}

// ForEach processes each group and window of events with the provided function
func (wgs *windowedGroupedStreamImpl) ForEach(processor func(string, *Window, []*Event)) error {
	// Start window manager
	ctx := context.Background()
	if err := wgs.windowManager.Start(ctx); err != nil {
		return err
	}
	defer wgs.windowManager.Stop()
	
	return wgs.groupedStream.baseStream.processor.processStream(wgs.groupedStream.baseStream.inputTopic, func(event *Event) error {
		// Apply base stream operations first
		processedEvent, shouldProcess := wgs.groupedStream.baseStream.applyOperations(event)
		if shouldProcess && processedEvent != nil {
			// Extract grouping key and window
			groupKey := wgs.groupedStream.keyExtractor(processedEvent)
			window := wgs.windowManager.GetWindow(processedEvent.Timestamp)
			windowKey := window.Start.Format(time.RFC3339)
			
			wgs.mu.Lock()
			if _, exists := wgs.groupWindows[groupKey]; !exists {
				wgs.groupWindows[groupKey] = make(map[string][]*Event)
			}
			if _, exists := wgs.groupWindows[groupKey][windowKey]; !exists {
				wgs.groupWindows[groupKey][windowKey] = make([]*Event, 0)
			}
			wgs.groupWindows[groupKey][windowKey] = append(wgs.groupWindows[groupKey][windowKey], processedEvent.Clone())
			events := make([]*Event, len(wgs.groupWindows[groupKey][windowKey]))
			copy(events, wgs.groupWindows[groupKey][windowKey])
			wgs.mu.Unlock()
			
			processor(groupKey, window, events)
			atomic.AddInt64(&wgs.metrics.EventsProcessed, 1)
		}
		return nil
	})
}

// Output sends windowed grouped events to the specified topic
func (wgs *windowedGroupedStreamImpl) Output(topic string) error {
	// Start window manager
	ctx := context.Background()
	if err := wgs.windowManager.Start(ctx); err != nil {
		return err
	}
	defer wgs.windowManager.Stop()
	
	return wgs.groupedStream.baseStream.processor.processStream(wgs.groupedStream.baseStream.inputTopic, func(event *Event) error {
		// Apply base stream operations first
		processedEvent, shouldProcess := wgs.groupedStream.baseStream.applyOperations(event)
		if shouldProcess && processedEvent != nil {
			// Extract grouping key and window
			groupKey := wgs.groupedStream.keyExtractor(processedEvent)
			window := wgs.windowManager.GetWindow(processedEvent.Timestamp)
			
			// Add grouping and window information to metadata
			if processedEvent.Metadata == nil {
				processedEvent.Metadata = make(map[string]interface{})
			}
			processedEvent.Metadata["group_key"] = groupKey
			processedEvent.Metadata["window_start"] = window.Start
			processedEvent.Metadata["window_end"] = window.End
			
			// Convert back to storage.Message and produce to output topic
			outputMsg := &storage.Message{
				Key:   processedEvent.Key,
				Value: processedEvent.Value,
			}
			
			_, _, err := wgs.groupedStream.baseStream.processor.storage.Produce(topic, outputMsg)
			if err != nil {
				atomic.AddInt64(&wgs.metrics.ProcessingErrors, 1)
				log.Printf("[WindowedGroupedStream] Failed to produce to output topic %s: %v", topic, err)
				return err
			}
			
			atomic.AddInt64(&wgs.metrics.EventsProcessed, 1)
		}
		return nil
	})
}

// Start begins processing the windowed grouped stream
func (wgs *windowedGroupedStreamImpl) Start(ctx context.Context) error {
	if err := wgs.windowManager.Start(ctx); err != nil {
		return err
	}
	return wgs.groupedStream.Start(ctx)
}

// Stop stops windowed grouped stream processing
func (wgs *windowedGroupedStreamImpl) Stop() error {
	wgs.windowManager.Stop()
	return wgs.groupedStream.Stop()
}

// GetMetrics returns the current processing metrics
func (wgs *windowedGroupedStreamImpl) GetMetrics() *ProcessorMetrics {
	wgs.mu.RLock()
	defer wgs.mu.RUnlock()
	
	// Combine metrics from grouped stream and windowed grouped stream
	groupedMetrics := wgs.groupedStream.GetMetrics()
	
	return &ProcessorMetrics{
		EventsProcessed:   groupedMetrics.EventsProcessed + atomic.LoadInt64(&wgs.metrics.EventsProcessed),
		EventsFiltered:    groupedMetrics.EventsFiltered + atomic.LoadInt64(&wgs.metrics.EventsFiltered),
		EventsTransformed: groupedMetrics.EventsTransformed + atomic.LoadInt64(&wgs.metrics.EventsTransformed),
		WindowsCreated:    atomic.LoadInt64(&wgs.metrics.WindowsCreated),
		WindowsExpired:    atomic.LoadInt64(&wgs.metrics.WindowsExpired),
		ProcessingErrors:  groupedMetrics.ProcessingErrors + atomic.LoadInt64(&wgs.metrics.ProcessingErrors),
		AvgLatency:        wgs.metrics.AvgLatency,
		ThroughputPerSec:  wgs.metrics.ThroughputPerSec,
	}
}