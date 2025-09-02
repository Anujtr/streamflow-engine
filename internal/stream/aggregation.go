package stream

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// aggregatedStreamImpl implements the AggregatedStream interface
type aggregatedStreamImpl struct {
	sourceStream     interface{} // Can be windowedStream, groupedStream, or windowedGroupedStream
	aggregationType  string
	valueExtractor   ValueExtractorFunc
	customAggregator AggregatorFunc
	resultFilters    []func(*AggregateResult) bool
	resultMappers    []func(*AggregateResult) *AggregateResult
	metrics          *ProcessorMetrics
	mu               sync.RWMutex
}

// NewAggregatedStream creates a new aggregated stream from a windowed stream
func NewAggregatedStream(windowedStream *windowedStreamImpl, aggregationType string, valueExtractor ValueExtractorFunc) *aggregatedStreamImpl {
	return &aggregatedStreamImpl{
		sourceStream:    windowedStream,
		aggregationType: aggregationType,
		valueExtractor:  valueExtractor,
		metrics:         &ProcessorMetrics{},
	}
}

// NewAggregatedStreamWithCustomAggregator creates an aggregated stream with custom aggregator
func NewAggregatedStreamWithCustomAggregator(windowedStream *windowedStreamImpl, aggregator AggregatorFunc) *aggregatedStreamImpl {
	return &aggregatedStreamImpl{
		sourceStream:     windowedStream,
		aggregationType:  "custom",
		customAggregator: aggregator,
		metrics:          &ProcessorMetrics{},
	}
}

// NewGroupedAggregatedStream creates a new aggregated stream from a grouped stream
func NewGroupedAggregatedStream(groupedStream *groupedStreamImpl, aggregationType string, valueExtractor ValueExtractorFunc) *aggregatedStreamImpl {
	return &aggregatedStreamImpl{
		sourceStream:    groupedStream,
		aggregationType: aggregationType,
		valueExtractor:  valueExtractor,
		metrics:         &ProcessorMetrics{},
	}
}

// NewGroupedAggregatedStreamWithCustomAggregator creates an aggregated stream with custom aggregator from grouped stream
func NewGroupedAggregatedStreamWithCustomAggregator(groupedStream *groupedStreamImpl, aggregator AggregatorFunc) *aggregatedStreamImpl {
	return &aggregatedStreamImpl{
		sourceStream:     groupedStream,
		aggregationType:  "custom",
		customAggregator: aggregator,
		metrics:          &ProcessorMetrics{},
	}
}

// NewWindowedGroupedAggregatedStream creates a new aggregated stream from a windowed grouped stream
func NewWindowedGroupedAggregatedStream(windowedGroupedStream *windowedGroupedStreamImpl, aggregationType string, valueExtractor ValueExtractorFunc) *aggregatedStreamImpl {
	return &aggregatedStreamImpl{
		sourceStream:    windowedGroupedStream,
		aggregationType: aggregationType,
		valueExtractor:  valueExtractor,
		metrics:         &ProcessorMetrics{},
	}
}

// NewWindowedGroupedAggregatedStreamWithCustomAggregator creates an aggregated stream with custom aggregator from windowed grouped stream
func NewWindowedGroupedAggregatedStreamWithCustomAggregator(windowedGroupedStream *windowedGroupedStreamImpl, aggregator AggregatorFunc) *aggregatedStreamImpl {
	return &aggregatedStreamImpl{
		sourceStream:     windowedGroupedStream,
		aggregationType:  "custom",
		customAggregator: aggregator,
		metrics:          &ProcessorMetrics{},
	}
}

// FilterResults adds a filter operation to aggregated results
func (as *aggregatedStreamImpl) FilterResults(filterFunc func(*AggregateResult) bool) AggregatedStream {
	as.mu.Lock()
	defer as.mu.Unlock()
	
	as.resultFilters = append(as.resultFilters, filterFunc)
	return as
}

// MapResults adds a map operation to aggregated results
func (as *aggregatedStreamImpl) MapResults(mapFunc func(*AggregateResult) *AggregateResult) AggregatedStream {
	as.mu.Lock()
	defer as.mu.Unlock()
	
	as.resultMappers = append(as.resultMappers, mapFunc)
	return as
}

// ForEach processes each aggregated result with the provided function
func (as *aggregatedStreamImpl) ForEach(processor func(*AggregateResult)) error {
	switch source := as.sourceStream.(type) {
	case *windowedStreamImpl:
		return as.processWindowedStream(source, processor)
	case *groupedStreamImpl:
		return as.processGroupedStream(source, processor)
	case *windowedGroupedStreamImpl:
		return as.processWindowedGroupedStream(source, processor)
	default:
		return fmt.Errorf("unsupported source stream type")
	}
}

// Output sends aggregated results to the specified topic
func (as *aggregatedStreamImpl) Output(topic string) error {
	return as.ForEach(func(result *AggregateResult) {
		// Get the processor from the source stream
		var processor *StreamProcessorImpl
		switch source := as.sourceStream.(type) {
		case *windowedStreamImpl:
			processor = source.baseStream.processor
		case *groupedStreamImpl:
			processor = source.baseStream.processor
		case *windowedGroupedStreamImpl:
			processor = source.groupedStream.baseStream.processor
		default:
			log.Printf("[AggregatedStream] Unsupported source stream type for output")
			return
		}
		
		if err := processor.PublishResult(topic, result); err != nil {
			atomic.AddInt64(&as.metrics.ProcessingErrors, 1)
			log.Printf("[AggregatedStream] Failed to publish result to topic %s: %v", topic, err)
		} else {
			atomic.AddInt64(&as.metrics.EventsProcessed, 1)
		}
	})
}

// Start begins processing the aggregated stream
func (as *aggregatedStreamImpl) Start(ctx context.Context) error {
	switch source := as.sourceStream.(type) {
	case *windowedStreamImpl:
		return source.Start(ctx)
	case *groupedStreamImpl:
		return source.Start(ctx)
	case *windowedGroupedStreamImpl:
		return source.Start(ctx)
	default:
		return fmt.Errorf("unsupported source stream type")
	}
}

// Stop stops aggregated stream processing
func (as *aggregatedStreamImpl) Stop() error {
	switch source := as.sourceStream.(type) {
	case *windowedStreamImpl:
		return source.Stop()
	case *groupedStreamImpl:
		return source.Stop()
	case *windowedGroupedStreamImpl:
		return source.Stop()
	default:
		return fmt.Errorf("unsupported source stream type")
	}
}

// GetMetrics returns the current processing metrics
func (as *aggregatedStreamImpl) GetMetrics() *ProcessorMetrics {
	as.mu.RLock()
	defer as.mu.RUnlock()
	
	var sourceMetrics *ProcessorMetrics
	switch source := as.sourceStream.(type) {
	case *windowedStreamImpl:
		sourceMetrics = source.GetMetrics()
	case *groupedStreamImpl:
		sourceMetrics = source.GetMetrics()
	case *windowedGroupedStreamImpl:
		sourceMetrics = source.GetMetrics()
	default:
		sourceMetrics = &ProcessorMetrics{}
	}
	
	return &ProcessorMetrics{
		EventsProcessed:   sourceMetrics.EventsProcessed + atomic.LoadInt64(&as.metrics.EventsProcessed),
		EventsFiltered:    sourceMetrics.EventsFiltered + atomic.LoadInt64(&as.metrics.EventsFiltered),
		EventsTransformed: sourceMetrics.EventsTransformed + atomic.LoadInt64(&as.metrics.EventsTransformed),
		EventsAggregated:  sourceMetrics.EventsAggregated + atomic.LoadInt64(&as.metrics.EventsAggregated),
		WindowsCreated:    sourceMetrics.WindowsCreated + atomic.LoadInt64(&as.metrics.WindowsCreated),
		WindowsExpired:    sourceMetrics.WindowsExpired + atomic.LoadInt64(&as.metrics.WindowsExpired),
		ProcessingErrors:  sourceMetrics.ProcessingErrors + atomic.LoadInt64(&as.metrics.ProcessingErrors),
		AvgLatency:        as.metrics.AvgLatency,
		ThroughputPerSec:  as.metrics.ThroughputPerSec,
	}
}

// processWindowedStream processes a windowed stream for aggregation
func (as *aggregatedStreamImpl) processWindowedStream(windowedStream *windowedStreamImpl, processor func(*AggregateResult)) error {
	// Use state store to track window aggregations
	stateStore := windowedStream.baseStream.processor.GetStateStore()
	
	return windowedStream.ForEach(func(windowedEvent *WindowedEvent) {
		windowKey := fmt.Sprintf("agg:%s:%s:%s", 
			as.aggregationType,
			windowedEvent.Window.Start.Format(time.RFC3339),
			windowedEvent.Window.End.Format(time.RFC3339))
		
		// Get existing state for this window
		var state *WindowState
		stateData, err := stateStore.GetWindowState(context.Background(), windowedEvent.Window, windowKey)
		if err != nil {
			log.Printf("[AggregatedStream] Failed to get window state: %v", err)
			return
		}
		
		if stateData != nil {
			state = &WindowState{}
			if err := state.FromJSON(stateData); err != nil {
				log.Printf("[AggregatedStream] Failed to parse window state: %v", err)
				state = as.createNewWindowState(windowedEvent.Window, "")
			}
		} else {
			state = as.createNewWindowState(windowedEvent.Window, "")
		}
		
		// Update state with new event
		if err := state.Update(windowedEvent.Event, as.valueExtractor); err != nil {
			log.Printf("[AggregatedStream] Failed to update window state: %v", err)
			return
		}
		
		// Save updated state
		stateJSON, err := state.ToJSON()
		if err != nil {
			log.Printf("[AggregatedStream] Failed to serialize window state: %v", err)
			return
		}
		
		if err := stateStore.PutWindowState(context.Background(), windowedEvent.Window, windowKey, stateJSON); err != nil {
			log.Printf("[AggregatedStream] Failed to save window state: %v", err)
			return
		}
		
		// Create aggregate result
		result := as.createAggregateResult(state, windowedEvent.Window, "")
		
		// Apply filters and mappers
		if as.shouldProcessResult(result) {
			result = as.applyResultMappers(result)
			if result != nil {
				processor(result)
				atomic.AddInt64(&as.metrics.EventsAggregated, 1)
			}
		}
	})
}

// processGroupedStream processes a grouped stream for aggregation
func (as *aggregatedStreamImpl) processGroupedStream(groupedStream *groupedStreamImpl, processor func(*AggregateResult)) error {
	stateStore := groupedStream.baseStream.processor.GetStateStore()
	
	return groupedStream.ForEach(func(groupKey string, events []*Event) {
		if len(events) == 0 {
			return
		}
		
		// Use the latest event to create a pseudo-window for state management
		latestEvent := events[len(events)-1]
		window := &Window{
			Start: latestEvent.Timestamp.Truncate(time.Minute),
			End:   latestEvent.Timestamp.Truncate(time.Minute).Add(time.Minute),
		}
		
		stateKey := fmt.Sprintf("group_agg:%s:%s", as.aggregationType, groupKey)
		
		// Get existing state for this group
		var state *WindowState
		stateData, err := stateStore.Get(context.Background(), stateKey)
		if err != nil {
			log.Printf("[AggregatedStream] Failed to get group state: %v", err)
			return
		}
		
		if stateData != nil {
			state = &WindowState{}
			if err := state.FromJSON(stateData); err != nil {
				log.Printf("[AggregatedStream] Failed to parse group state: %v", err)
				state = as.createNewWindowState(window, groupKey)
			}
		} else {
			state = as.createNewWindowState(window, groupKey)
		}
		
		// Update state with the latest event
		if err := state.Update(latestEvent, as.valueExtractor); err != nil {
			log.Printf("[AggregatedStream] Failed to update group state: %v", err)
			return
		}
		
		// Save updated state
		stateJSON, err := state.ToJSON()
		if err != nil {
			log.Printf("[AggregatedStream] Failed to serialize group state: %v", err)
			return
		}
		
		if err := stateStore.Put(context.Background(), stateKey, stateJSON); err != nil {
			log.Printf("[AggregatedStream] Failed to save group state: %v", err)
			return
		}
		
		// Create aggregate result
		result := as.createAggregateResult(state, window, groupKey)
		
		// Apply filters and mappers
		if as.shouldProcessResult(result) {
			result = as.applyResultMappers(result)
			if result != nil {
				processor(result)
				atomic.AddInt64(&as.metrics.EventsAggregated, 1)
			}
		}
	})
}

// processWindowedGroupedStream processes a windowed grouped stream for aggregation
func (as *aggregatedStreamImpl) processWindowedGroupedStream(windowedGroupedStream *windowedGroupedStreamImpl, processor func(*AggregateResult)) error {
	stateStore := windowedGroupedStream.groupedStream.baseStream.processor.GetStateStore()
	
	return windowedGroupedStream.ForEach(func(groupKey string, window *Window, events []*Event) {
		if len(events) == 0 {
			return
		}
		
		stateKey := fmt.Sprintf("windowed_group_agg:%s:%s", as.aggregationType, groupKey)
		
		// Get existing state for this group and window
		var state *WindowState
		stateData, err := stateStore.GetWindowState(context.Background(), window, stateKey)
		if err != nil {
			log.Printf("[AggregatedStream] Failed to get windowed group state: %v", err)
			return
		}
		
		if stateData != nil {
			state = &WindowState{}
			if err := state.FromJSON(stateData); err != nil {
				log.Printf("[AggregatedStream] Failed to parse windowed group state: %v", err)
				state = as.createNewWindowState(window, groupKey)
			}
		} else {
			state = as.createNewWindowState(window, groupKey)
		}
		
		// Update state with the latest event
		latestEvent := events[len(events)-1]
		if err := state.Update(latestEvent, as.valueExtractor); err != nil {
			log.Printf("[AggregatedStream] Failed to update windowed group state: %v", err)
			return
		}
		
		// Save updated state
		stateJSON, err := state.ToJSON()
		if err != nil {
			log.Printf("[AggregatedStream] Failed to serialize windowed group state: %v", err)
			return
		}
		
		if err := stateStore.PutWindowState(context.Background(), window, stateKey, stateJSON); err != nil {
			log.Printf("[AggregatedStream] Failed to save windowed group state: %v", err)
			return
		}
		
		// Create aggregate result
		result := as.createAggregateResult(state, window, groupKey)
		
		// Apply filters and mappers
		if as.shouldProcessResult(result) {
			result = as.applyResultMappers(result)
			if result != nil {
				processor(result)
				atomic.AddInt64(&as.metrics.EventsAggregated, 1)
			}
		}
	})
}

// createNewWindowState creates a new window state
func (as *aggregatedStreamImpl) createNewWindowState(window *Window, groupKey string) *WindowState {
	return &WindowState{
		Key:       groupKey,
		Window:    window,
		Count:     0,
		Sum:       0,
		Min:       math.MaxFloat64,
		Max:       math.SmallestNonzeroFloat64,
		Events:    make([]*Event, 0),
		Metadata:  make(map[string]interface{}),
		UpdatedAt: time.Now(),
	}
}

// createAggregateResult creates an aggregate result from window state
func (as *aggregatedStreamImpl) createAggregateResult(state *WindowState, window *Window, groupKey string) *AggregateResult {
	result := &AggregateResult{
		Key:       groupKey,
		Window:    window,
		Count:     state.Count,
		Timestamp: state.UpdatedAt,
		Metadata:  make(map[string]interface{}),
	}
	
	// Copy metadata from state
	for k, v := range state.Metadata {
		result.Metadata[k] = v
	}
	
	// Set aggregated value based on type
	switch as.aggregationType {
	case "count":
		result.Count = state.Count
	case "sum":
		result.Sum = state.Sum
	case "average":
		result.Average = state.GetAverage()
		result.Sum = state.Sum
	case "min":
		if state.Count > 0 {
			result.Min = state.Min
		} else {
			result.Min = 0
		}
	case "max":
		if state.Count > 0 {
			result.Max = state.Max
		} else {
			result.Max = 0
		}
	case "custom":
		if as.customAggregator != nil {
			// Apply custom aggregator to all events in the state
			var events []*Event
			if state.Events != nil {
				events = state.Events
			} else {
				events = []*Event{} // Empty slice for custom aggregator
			}
			customResult := as.customAggregator(events)
			if customResult != nil {
				// Copy all fields from custom result
				result.Count = customResult.Count
				result.Sum = customResult.Sum
				result.Average = customResult.Average
				result.Min = customResult.Min
				result.Max = customResult.Max
				// Merge metadata from custom result
				for k, v := range customResult.Metadata {
					result.Metadata[k] = v
				}
			}
		}
	}
	
	// Add aggregation type to metadata
	result.Metadata["aggregation_type"] = as.aggregationType
	
	return result
}

// shouldProcessResult applies filters to determine if result should be processed
func (as *aggregatedStreamImpl) shouldProcessResult(result *AggregateResult) bool {
	for _, filter := range as.resultFilters {
		if !filter(result) {
			return false
		}
	}
	return true
}

// applyResultMappers applies all result mappers to transform the result
func (as *aggregatedStreamImpl) applyResultMappers(result *AggregateResult) *AggregateResult {
	currentResult := result
	for _, mapper := range as.resultMappers {
		currentResult = mapper(currentResult)
		if currentResult == nil {
			break
		}
	}
	return currentResult
}