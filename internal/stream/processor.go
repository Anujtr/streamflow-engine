package stream

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/storage"
	"github.com/Anujtr/streamflow-engine/pkg/client"
)

// StreamProcessorImpl implements the StreamProcessor interface
type StreamProcessorImpl struct {
	config      *ProcessorConfig
	storage     *storage.Storage
	consumer    *client.ManagedConsumer
	producer    *client.Producer
	stateStore  StateStore
	
	// Runtime state
	isRunning   bool
	stopChan    chan struct{}
	wg          sync.WaitGroup
	mu          sync.RWMutex
	
	// Metrics
	metrics     *ProcessorMetrics
	startTime   time.Time
	
	// Processing
	eventChan   chan *Event
	errorChan   chan error
}

// NewStreamProcessor creates a new stream processor
func NewStreamProcessor(config *ProcessorConfig, storage *storage.Storage) (*StreamProcessorImpl, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	
	if config.ProcessorName == "" {
		config.ProcessorName = fmt.Sprintf("processor-%d", time.Now().Unix())
	}
	
	if config.ConsumerGroup == "" {
		config.ConsumerGroup = config.ProcessorName + "-group"
	}
	
	if config.MaxConcurrency <= 0 {
		config.MaxConcurrency = 4
	}
	
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	
	if config.FlushInterval <= 0 {
		config.FlushInterval = 1 * time.Second
	}
	
	// Create state store
	var stateStore StateStore
	var err error
	
	if config.StateStoreType == "pebble" && config.StateStorePath != "" {
		stateStore, err = NewPebbleStateStore(config.StateStorePath)
		if err != nil {
			return nil, fmt.Errorf("failed to create Pebble state store: %v", err)
		}
	} else {
		stateStore = NewMemoryStateStore()
	}
	
	processor := &StreamProcessorImpl{
		config:     config,
		storage:    storage,
		stateStore: stateStore,
		stopChan:   make(chan struct{}),
		metrics:    &ProcessorMetrics{},
		eventChan:  make(chan *Event, config.BatchSize*2),
		errorChan:  make(chan error, 100),
	}
	
	return processor, nil
}

// NewStream creates a new stream processing pipeline
func (sp *StreamProcessorImpl) NewStream(inputTopic string) Stream {
	return NewStream(inputTopic, sp)
}

// Start begins stream processing
func (sp *StreamProcessorImpl) Start(ctx context.Context) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	
	if sp.isRunning {
		return fmt.Errorf("processor is already running")
	}
	
	log.Printf("[StreamProcessor] Starting processor %s", sp.config.ProcessorName)
	
	// Create consumer
	consumerConfig := client.ManagedConsumerConfig{
		Address:                "localhost:8080", // TODO: Make configurable
		Timeout:                30 * time.Second,
		MaxMessages:            int32(sp.config.BatchSize),
		ConsumerGroup:          sp.config.ConsumerGroup,
		EnableAutoCommit:       true,
		AutoCommitInterval:     5 * time.Second,
		EnableAutoOffsetStore:  true,
	}
	
	consumer, err := client.NewManagedConsumer(consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %v", err)
	}
	sp.consumer = consumer
	
	// Create producer
	producerConfig := client.ProducerConfig{
		Address: "localhost:8080", // TODO: Make configurable
		Timeout: 30 * time.Second,
	}
	
	producer, err := client.NewProducer(producerConfig)
	if err != nil {
		consumer.Close()
		return fmt.Errorf("failed to create producer: %v", err)
	}
	sp.producer = producer
	
	sp.isRunning = true
	sp.startTime = time.Now()
	
	// Start error handler
	sp.wg.Add(1)
	go sp.errorHandler(ctx)
	
	// Start metrics updater
	sp.wg.Add(1)
	go sp.metricsUpdater(ctx)
	
	log.Printf("[StreamProcessor] Processor %s started successfully", sp.config.ProcessorName)
	return nil
}

// Stop stops stream processing
func (sp *StreamProcessorImpl) Stop() error {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	
	if !sp.isRunning {
		return nil
	}
	
	log.Printf("[StreamProcessor] Stopping processor %s", sp.config.ProcessorName)
	
	sp.isRunning = false
	close(sp.stopChan)
	
	// Close consumer and producer
	if sp.consumer != nil {
		sp.consumer.Close()
	}
	if sp.producer != nil {
		sp.producer.Close()
	}
	
	// Close state store
	if sp.stateStore != nil {
		sp.stateStore.Close()
	}
	
	// Wait for all goroutines to finish
	sp.wg.Wait()
	
	log.Printf("[StreamProcessor] Processor %s stopped", sp.config.ProcessorName)
	return nil
}

// IsRunning returns whether the processor is currently running
func (sp *StreamProcessorImpl) IsRunning() bool {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.isRunning
}

// GetConfig returns the processor configuration
func (sp *StreamProcessorImpl) GetConfig() *ProcessorConfig {
	return sp.config
}

// GetMetrics returns the current processor metrics
func (sp *StreamProcessorImpl) GetMetrics() *ProcessorMetrics {
	return &ProcessorMetrics{
		EventsProcessed:   atomic.LoadInt64(&sp.metrics.EventsProcessed),
		EventsFiltered:    atomic.LoadInt64(&sp.metrics.EventsFiltered),
		EventsTransformed: atomic.LoadInt64(&sp.metrics.EventsTransformed),
		EventsAggregated:  atomic.LoadInt64(&sp.metrics.EventsAggregated),
		WindowsCreated:    atomic.LoadInt64(&sp.metrics.WindowsCreated),
		WindowsExpired:    atomic.LoadInt64(&sp.metrics.WindowsExpired),
		ProcessingErrors:  atomic.LoadInt64(&sp.metrics.ProcessingErrors),
		AvgLatency:        sp.metrics.AvgLatency,
		ThroughputPerSec:  sp.metrics.ThroughputPerSec,
	}
}

// GetHealthStatus returns the health status of the processor
func (sp *StreamProcessorImpl) GetHealthStatus() string {
	if sp.IsRunning() {
		return "healthy"
	}
	return "stopped"
}

// processStream processes events from an input topic using the provided processor function
func (sp *StreamProcessorImpl) processStream(inputTopic string, processor func(*Event) error) error {
	if !sp.IsRunning() {
		return fmt.Errorf("processor is not running")
	}
	
	log.Printf("[StreamProcessor] Starting to process stream from topic: %s", inputTopic)
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Start processing workers
	for i := 0; i < sp.config.MaxConcurrency; i++ {
		sp.wg.Add(1)
		go func(workerID int) {
			defer sp.wg.Done()
			sp.processingWorker(ctx, workerID, processor)
		}(i)
	}
	
	// Start consuming events
	sp.wg.Add(1)
	go func() {
		defer sp.wg.Done()
		sp.eventConsumer(ctx, inputTopic)
	}()
	
	// Wait for stop signal
	select {
	case <-sp.stopChan:
		cancel()
	case <-ctx.Done():
	}
	
	return nil
}

// eventConsumer consumes events from the input topic and sends them to the event channel
func (sp *StreamProcessorImpl) eventConsumer(ctx context.Context, inputTopic string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[StreamProcessor] Panic in event consumer: %v", r)
		}
	}()
	
	partition := int32(0) // TODO: Support multiple partitions
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-sp.stopChan:
			return
		default:
		}
		
		// Consume messages from the input topic
		messages, err := sp.consumer.ConsumeFromCommittedOffset(ctx, inputTopic, partition, int32(sp.config.BatchSize))
		if err != nil {
			select {
			case sp.errorChan <- fmt.Errorf("failed to consume from topic %s: %v", inputTopic, err):
			default:
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		
		if len(messages) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		
		// Convert messages to events and send to processing channel
		for _, msg := range messages {
			event := &Event{
				Key:       msg.Key,
				Value:     msg.Value,
				Headers:   make(map[string]string),
				Timestamp: msg.Timestamp,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Topic:     inputTopic,
				Metadata:  make(map[string]interface{}),
			}
			
			// Note: ConsumeMessage doesn't have headers, but we initialize empty headers map for events
			
			select {
			case sp.eventChan <- event:
				// Event successfully queued for processing
			case <-ctx.Done():
				return
			case <-sp.stopChan:
				return
			default:
				// Channel is full, drop the event and log error
				atomic.AddInt64(&sp.metrics.ProcessingErrors, 1)
				log.Printf("[StreamProcessor] Event channel full, dropping event from %s:%d at offset %d", 
					inputTopic, msg.Partition, msg.Offset)
			}
		}
	}
}

// processingWorker processes events from the event channel
func (sp *StreamProcessorImpl) processingWorker(ctx context.Context, workerID int, processor func(*Event) error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[StreamProcessor] Panic in processing worker %d: %v", workerID, r)
		}
	}()
	
	// Track processed events for offset commit
	processedEvents := make([]*Event, 0, sp.config.BatchSize)
	commitTicker := time.NewTicker(sp.config.FlushInterval)
	defer commitTicker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			// Commit any remaining processed events before shutdown
			sp.commitProcessedEvents(ctx, processedEvents)
			return
		case <-sp.stopChan:
			// Commit any remaining processed events before shutdown
			sp.commitProcessedEvents(ctx, processedEvents)
			return
		case <-commitTicker.C:
			// Periodically commit processed events
			if len(processedEvents) > 0 {
				sp.commitProcessedEvents(ctx, processedEvents)
				processedEvents = processedEvents[:0] // Clear slice but keep capacity
			}
		case event := <-sp.eventChan:
			if event == nil {
				continue
			}
			
			start := time.Now()
			
			// Process the event
			if err := processor(event); err != nil {
				atomic.AddInt64(&sp.metrics.ProcessingErrors, 1)
				select {
				case sp.errorChan <- fmt.Errorf("processing error in worker %d: %v", workerID, err):
				default:
				}
			} else {
				// Only track successfully processed events for commit
				processedEvents = append(processedEvents, event)
				atomic.AddInt64(&sp.metrics.EventsProcessed, 1)
				
				// Commit batch when full
				if len(processedEvents) >= sp.config.BatchSize {
					sp.commitProcessedEvents(ctx, processedEvents)
					processedEvents = processedEvents[:0] // Clear slice but keep capacity
				}
			}
			
			// Update latency metrics
			latency := time.Since(start)
			sp.updateLatency(latency)
		}
	}
}

// errorHandler handles errors from processing
func (sp *StreamProcessorImpl) errorHandler(ctx context.Context) {
	defer sp.wg.Done()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-sp.stopChan:
			return
		case err := <-sp.errorChan:
			if err != nil {
				log.Printf("[StreamProcessor] Error: %v", err)
				
				// TODO: Implement error topic publishing if configured
				if sp.config.ErrorTopic != "" {
					// Would publish error to error topic
				}
			}
		}
	}
}

// metricsUpdater periodically updates throughput metrics
func (sp *StreamProcessorImpl) metricsUpdater(ctx context.Context) {
	defer sp.wg.Done()
	
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	var lastProcessed int64
	lastUpdate := time.Now()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-sp.stopChan:
			return
		case <-ticker.C:
			currentProcessed := atomic.LoadInt64(&sp.metrics.EventsProcessed)
			currentTime := time.Now()
			
			// Calculate throughput
			processingDelta := currentProcessed - lastProcessed
			timeDelta := currentTime.Sub(lastUpdate).Seconds()
			
			if timeDelta > 0 {
				throughput := float64(processingDelta) / timeDelta
				sp.metrics.ThroughputPerSec = throughput
			}
			
			lastProcessed = currentProcessed
			lastUpdate = currentTime
		}
	}
}

// updateLatency updates the average latency metric
func (sp *StreamProcessorImpl) updateLatency(latency time.Duration) {
	// Simple moving average (could be improved with more sophisticated metrics)
	currentAvg := sp.metrics.AvgLatency
	if currentAvg == 0 {
		sp.metrics.AvgLatency = latency
	} else {
		// Exponential moving average with alpha = 0.1
		sp.metrics.AvgLatency = time.Duration(0.9*float64(currentAvg) + 0.1*float64(latency))
	}
}

// GetStateStore returns the processor's state store
func (sp *StreamProcessorImpl) GetStateStore() StateStore {
	return sp.stateStore
}

// commitProcessedEvents commits offsets for successfully processed events
func (sp *StreamProcessorImpl) commitProcessedEvents(ctx context.Context, events []*Event) {
	if len(events) == 0 {
		return
	}
	
	// Group events by topic and partition to find the highest offset for each
	offsetMap := make(map[string]map[int32]int64) // topic -> partition -> highest_offset
	
	for _, event := range events {
		if _, exists := offsetMap[event.Topic]; !exists {
			offsetMap[event.Topic] = make(map[int32]int64)
		}
		
		// Keep track of the highest offset for each topic-partition
		if event.Offset > offsetMap[event.Topic][event.Partition] {
			offsetMap[event.Topic][event.Partition] = event.Offset
		}
	}
	
	// Commit the highest offset + 1 for each topic-partition
	for topic, partitions := range offsetMap {
		for partition, offset := range partitions {
			// Commit offset + 1 since we want to consume from the next message
			nextOffset := offset + 1
			if err := sp.consumer.CommitOffset(ctx, topic, partition, nextOffset); err != nil {
				log.Printf("[StreamProcessor] Failed to commit offset %d for %s:%d: %v", nextOffset, topic, partition, err)
				atomic.AddInt64(&sp.metrics.ProcessingErrors, 1)
			} else {
				log.Printf("[StreamProcessor] Successfully committed offset %d for %s:%d", nextOffset, topic, partition)
			}
		}
	}
}

// PublishResult publishes a processing result to an output topic
func (sp *StreamProcessorImpl) PublishResult(topic string, result *AggregateResult) error {
	// Convert aggregate result to a message
	resultEvent := &Event{
		Key:       result.Key,
		Headers:   make(map[string]string),
		Timestamp: result.Timestamp,
		Topic:     topic,
		Metadata:  result.Metadata,
	}
	
	// Set value as JSON
	if err := resultEvent.SetValueFromJSON(result); err != nil {
		return fmt.Errorf("failed to serialize result: %v", err)
	}
	
	// Add headers for result metadata
	if resultEvent.Headers == nil {
		resultEvent.Headers = make(map[string]string)
	}
	resultEvent.Headers["result-type"] = "aggregation"
	if result.Window != nil {
		resultEvent.Headers["window-start"] = result.Window.Start.Format(time.RFC3339)
		resultEvent.Headers["window-end"] = result.Window.End.Format(time.RFC3339)
	}
	
	// Produce to output topic
	_, err := sp.producer.SendSingle(context.Background(), topic, resultEvent.Key, resultEvent.Value)
	if err != nil {
		atomic.AddInt64(&sp.metrics.ProcessingErrors, 1)
		return fmt.Errorf("failed to publish result to topic %s: %v", topic, err)
	}
	
	return nil
}