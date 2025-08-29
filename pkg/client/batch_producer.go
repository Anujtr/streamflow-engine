package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
)

// BatchProducer provides high-throughput batched message production
type BatchProducer struct {
	client   pb.MessageServiceClient
	config   BatchProducerConfig
	
	// Batching state
	batches  map[string]*topicBatch // topic -> batch
	batchMu  sync.RWMutex
	
	// Background processing
	flushTicker *time.Ticker
	stopCh      chan struct{}
	wg          sync.WaitGroup
	
	// Metrics
	batchesSent     int64
	messagesSent    int64
	bytesProduced   int64
	flushCount      int64
	errors          int64
}

// BatchProducerConfig configures the batch producer
type BatchProducerConfig struct {
	// Batching parameters
	MaxBatchSize     int           `json:"max_batch_size"`     // Maximum messages per batch
	MaxBatchBytes    int           `json:"max_batch_bytes"`    // Maximum bytes per batch
	BatchTimeout     time.Duration `json:"batch_timeout"`     // Maximum time to wait before flushing
	FlushInterval    time.Duration `json:"flush_interval"`    // Background flush interval
	
	// Retry configuration
	RetryAttempts    int           `json:"retry_attempts"`    // Number of retry attempts
	RetryBackoff     time.Duration `json:"retry_backoff"`     // Initial retry backoff
	
	// Compression (future enhancement)
	EnableCompression bool `json:"enable_compression"`
}

// topicBatch holds batched messages for a single topic
type topicBatch struct {
	topic     string
	messages  []*pb.Message
	totalBytes int
	createdAt time.Time
	mu        sync.Mutex
}

// NewBatchProducer creates a new batch producer
func NewBatchProducer(client pb.MessageServiceClient, config BatchProducerConfig) *BatchProducer {
	// Set defaults
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 1000
	}
	if config.MaxBatchBytes <= 0 {
		config.MaxBatchBytes = 1024 * 1024 // 1MB
	}
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = 100 * time.Millisecond
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 50 * time.Millisecond
	}
	if config.RetryAttempts <= 0 {
		config.RetryAttempts = 3
	}
	if config.RetryBackoff <= 0 {
		config.RetryBackoff = 10 * time.Millisecond
	}

	bp := &BatchProducer{
		client:      client,
		config:      config,
		batches:     make(map[string]*topicBatch),
		flushTicker: time.NewTicker(config.FlushInterval),
		stopCh:      make(chan struct{}),
	}

	// Start background flush goroutine
	bp.wg.Add(1)
	go bp.backgroundFlush()

	return bp
}

// SendAsync adds a message to the batch (non-blocking)
func (bp *BatchProducer) SendAsync(ctx context.Context, topic, key string, value []byte) error {
	if topic == "" {
		return ErrInvalidTopic
	}

	message := &pb.Message{
		Key:   key,
		Value: value,
	}

	messageSize := len(key) + len(value)

	bp.batchMu.Lock()
	batch, exists := bp.batches[topic]
	if !exists {
		batch = &topicBatch{
			topic:     topic,
			messages:  make([]*pb.Message, 0, bp.config.MaxBatchSize),
			createdAt: time.Now(),
		}
		bp.batches[topic] = batch
	}
	bp.batchMu.Unlock()

	batch.mu.Lock()
	defer batch.mu.Unlock()

	// Check if adding this message would exceed limits
	wouldExceedSize := len(batch.messages) >= bp.config.MaxBatchSize
	wouldExceedBytes := batch.totalBytes+messageSize > bp.config.MaxBatchBytes
	
	if wouldExceedSize || wouldExceedBytes {
		// Flush current batch before adding new message
		batch.mu.Unlock()
		bp.flushBatch(ctx, topic)
		batch.mu.Lock()
		
		// Reset batch
		batch.messages = make([]*pb.Message, 0, bp.config.MaxBatchSize)
		batch.totalBytes = 0
		batch.createdAt = time.Now()
	}

	// Add message to batch
	batch.messages = append(batch.messages, message)
	batch.totalBytes += messageSize

	return nil
}

// Flush immediately flushes all batches
func (bp *BatchProducer) Flush(ctx context.Context) error {
	bp.batchMu.RLock()
	topics := make([]string, 0, len(bp.batches))
	for topic := range bp.batches {
		topics = append(topics, topic)
	}
	bp.batchMu.RUnlock()

	var lastErr error
	for _, topic := range topics {
		if err := bp.flushBatch(ctx, topic); err != nil {
			lastErr = err
		}
	}

	atomic.AddInt64(&bp.flushCount, 1)
	return lastErr
}

// flushBatch flushes a specific topic batch
func (bp *BatchProducer) flushBatch(ctx context.Context, topic string) error {
	bp.batchMu.Lock()
	batch, exists := bp.batches[topic]
	if !exists {
		bp.batchMu.Unlock()
		return nil
	}
	bp.batchMu.Unlock()

	batch.mu.Lock()
	if len(batch.messages) == 0 {
		batch.mu.Unlock()
		return nil
	}

	// Copy messages to avoid holding lock during network call
	messages := make([]*pb.Message, len(batch.messages))
	copy(messages, batch.messages)
	
	// Reset batch
	batch.messages = batch.messages[:0]
	batch.totalBytes = 0
	batch.createdAt = time.Now()
	batch.mu.Unlock()

	// Send batch with retries
	err := bp.sendBatchWithRetry(ctx, topic, messages)
	if err != nil {
		atomic.AddInt64(&bp.errors, 1)
		return err
	}

	// Update metrics
	atomic.AddInt64(&bp.batchesSent, 1)
	atomic.AddInt64(&bp.messagesSent, int64(len(messages)))
	
	totalBytes := int64(0)
	for _, msg := range messages {
		totalBytes += int64(len(msg.Key) + len(msg.Value))
	}
	atomic.AddInt64(&bp.bytesProduced, totalBytes)

	return nil
}

// sendBatchWithRetry sends a batch with exponential backoff retry
func (bp *BatchProducer) sendBatchWithRetry(ctx context.Context, topic string, messages []*pb.Message) error {
	req := &pb.ProduceRequest{
		Topic:    topic,
		Messages: messages,
	}

	var lastErr error
	backoff := bp.config.RetryBackoff

	for attempt := 0; attempt <= bp.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2 // Exponential backoff
		}

		_, err := bp.client.Produce(ctx, req)
		if err == nil {
			return nil
		}

		lastErr = err

		// Don't retry on context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	return lastErr
}

// backgroundFlush runs periodic batch flushing
func (bp *BatchProducer) backgroundFlush() {
	defer bp.wg.Done()

	for {
		select {
		case <-bp.flushTicker.C:
			bp.flushStaleBatches()
		case <-bp.stopCh:
			return
		}
	}
}

// flushStaleBatches flushes batches that have exceeded the timeout
func (bp *BatchProducer) flushStaleBatches() {
	ctx, cancel := context.WithTimeout(context.Background(), bp.config.BatchTimeout)
	defer cancel()

	bp.batchMu.RLock()
	staleBatches := make([]string, 0)
	now := time.Now()

	for topic, batch := range bp.batches {
		batch.mu.Lock()
		if len(batch.messages) > 0 && now.Sub(batch.createdAt) >= bp.config.BatchTimeout {
			staleBatches = append(staleBatches, topic)
		}
		batch.mu.Unlock()
	}
	bp.batchMu.RUnlock()

	// Flush stale batches
	for _, topic := range staleBatches {
		bp.flushBatch(ctx, topic)
	}
}

// Close gracefully shuts down the batch producer
func (bp *BatchProducer) Close(ctx context.Context) error {
	// Stop background flush
	close(bp.stopCh)
	bp.flushTicker.Stop()

	// Flush all remaining batches
	bp.Flush(ctx)

	// Wait for background goroutine to finish
	bp.wg.Wait()

	return nil
}

// GetMetrics returns batch producer metrics
func (bp *BatchProducer) GetMetrics() BatchProducerMetrics {
	return BatchProducerMetrics{
		BatchesSent:   atomic.LoadInt64(&bp.batchesSent),
		MessagesSent:  atomic.LoadInt64(&bp.messagesSent),
		BytesProduced: atomic.LoadInt64(&bp.bytesProduced),
		FlushCount:    atomic.LoadInt64(&bp.flushCount),
		Errors:        atomic.LoadInt64(&bp.errors),
		ActiveBatches: bp.getActiveBatchCount(),
	}
}

func (bp *BatchProducer) getActiveBatchCount() int {
	bp.batchMu.RLock()
	defer bp.batchMu.RUnlock()
	
	count := 0
	for _, batch := range bp.batches {
		batch.mu.Lock()
		if len(batch.messages) > 0 {
			count++
		}
		batch.mu.Unlock()
	}
	return count
}

// BatchProducerMetrics contains batch producer performance metrics
type BatchProducerMetrics struct {
	BatchesSent   int64 `json:"batches_sent"`
	MessagesSent  int64 `json:"messages_sent"`
	BytesProduced int64 `json:"bytes_produced"`
	FlushCount    int64 `json:"flush_count"`
	Errors        int64 `json:"errors"`
	ActiveBatches int   `json:"active_batches"`
}

// Common errors
var (
	ErrInvalidTopic = fmt.Errorf("topic name cannot be empty")
)