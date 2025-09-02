package persistence

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/Anujtr/streamflow-engine/internal/metrics"
	"github.com/cockroachdb/pebble"
)

// PebbleStorage provides persistent storage using Pebble database
type PebbleStorage struct {
	db         *pebble.DB
	dataDir    string
	mu         sync.RWMutex
	closed     bool
	
	// Batch processing for high throughput
	batchSize     int
	batchTimeout  time.Duration
	pendingWrites chan *batchWrite
	batchWorkers  int
	stopBatching  chan struct{}
	batchWG       sync.WaitGroup
	
	// Per-partition offset allocation to reduce contention
	partitionMutexes sync.Map // map[string]*sync.Mutex for topic:partition keys
	offsetCache     sync.Map // map[string]*int64 for cached next offsets
	
	// Performance metrics
	metrics *metrics.PerformanceMetrics
}

type batchWrite struct {
	topicName string
	partition int32
	msg       *Message
	resultCh  chan batchResult
}

type batchResult struct {
	offset int64
	err    error
}

// NewPebbleStorage creates a new Pebble-based storage instance
func NewPebbleStorage(dataDir string) (*PebbleStorage, error) {
	// Create data directory if it doesn't exist
	dbPath := filepath.Join(dataDir, "pebble-db")
	
	// Configure Pebble options optimized for high write throughput
	opts := &pebble.Options{
		Cache:                       pebble.NewCache(256 << 20), // 256MB cache (increased)
		MemTableSize:                128 << 20,                  // 128MB memtable (increased) 
		MemTableStopWritesThreshold: 4,                          // Allow more memtables
		MaxConcurrentCompactions:    func() int { return 8 },    // More concurrent compactions
		L0StopWritesThreshold:       48,                         // Higher threshold before blocking writes
		L0CompactionThreshold:       8,                          // Start compacting earlier
		LBaseMaxBytes:               512 << 20,                  // 512MB (increased)
	}

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open Pebble database: %v", err)
	}

	storage := &PebbleStorage{
		db:            db,
		dataDir:       dataDir,
		batchSize:     100,                      // Batch up to 100 messages
		batchTimeout:  10 * time.Millisecond,    // Or flush every 10ms
		pendingWrites: make(chan *batchWrite, 1000),
		batchWorkers:  4,                        // 4 batch workers
		stopBatching:  make(chan struct{}),
		metrics:       metrics.NewPerformanceMetrics(),
	}
	
	// Start batch workers for high-throughput writes
	for i := 0; i < storage.batchWorkers; i++ {
		storage.batchWG.Add(1)
		go storage.batchWorker(i) // Pass worker ID for debugging
	}
	
	return storage, nil
}

// Close closes the Pebble database
func (ps *PebbleStorage) Close() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.closed {
		return nil
	}

	ps.closed = true
	
	// Stop batch workers
	close(ps.stopBatching)
	ps.batchWG.Wait()
	close(ps.pendingWrites)
	
	return ps.db.Close()
}

// GetDB returns the underlying Pebble database instance
func (ps *PebbleStorage) GetDB() *pebble.DB {
	return ps.db
}

// Message represents a message in persistent storage
type Message struct {
	Key   string
	Value []byte
}

// Append appends a message to a partition and returns the assigned offset
func (ps *PebbleStorage) Append(topicName string, partition int32, msg *Message) (int64, error) {
	start := time.Now()
	defer func() {
		ps.metrics.RecordProduceLatency(time.Since(start))
	}()
	
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if ps.closed {
		ps.metrics.IncrementProduceErrors()
		return -1, fmt.Errorf("storage is closed")
	}

	// Get next offset for this partition
	offset, err := ps.getNextOffset(topicName, partition)
	if err != nil {
		ps.metrics.IncrementProduceErrors()
		return -1, fmt.Errorf("failed to get next offset: %v", err)
	}

	// Create the key: topic:partition:offset
	key := makeMessageKey(topicName, partition, offset)
	
	// Serialize the message with timestamp
	messageData := &PersistedMessage{
		Key:       msg.Key,
		Value:     msg.Value,
		Timestamp: time.Now(),
		Offset:    offset,
	}
	
	value, err := json.Marshal(messageData)
	if err != nil {
		return -1, fmt.Errorf("failed to serialize message: %v", err)
	}

	// Write to Pebble with sync for durability
	batch := ps.db.NewBatch()
	defer batch.Close()

	// Write the message
	if err := batch.Set(key, value, pebble.Sync); err != nil {
		return -1, fmt.Errorf("failed to write message: %v", err)
	}

	// Update the next offset
	offsetKey := makeOffsetKey(topicName, partition)
	offsetValue := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetValue, uint64(offset+1))
	if err := batch.Set(offsetKey, offsetValue, pebble.Sync); err != nil {
		return -1, fmt.Errorf("failed to update offset: %v", err)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return -1, fmt.Errorf("failed to commit batch: %v", err)
	}

	return offset, nil
}

// AppendBatched appends a message using the high-throughput batch processing system
func (ps *PebbleStorage) AppendBatched(topicName string, partition int32, msg *Message) (int64, error) {
	start := time.Now()
	defer func() {
		ps.metrics.RecordProduceLatency(time.Since(start))
	}()
	
	ps.mu.RLock()
	if ps.closed {
		ps.mu.RUnlock()
		ps.metrics.IncrementProduceErrors()
		return -1, fmt.Errorf("storage is closed")
	}
	ps.mu.RUnlock()

	resultCh := make(chan batchResult, 1)
	write := &batchWrite{
		topicName: topicName,
		partition: partition,
		msg:       msg,
		resultCh:  resultCh,
	}

	// Send to batch processing queue with timeout
	select {
	case ps.pendingWrites <- write:
		// Successfully queued
	default:
		// Queue is full, fall back to synchronous write with logging
		log.Printf("[PebbleStorage] Batch queue full, falling back to synchronous write for %s:%d", topicName, partition)
		ps.metrics.IncrementProduceErrors() // Track queue overflow
		return ps.Append(topicName, partition, msg)
	}

	// Wait for batch processing result with timeout
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	
	select {
	case result := <-resultCh:
		if result.err != nil {
			ps.metrics.IncrementProduceErrors()
		}
		return result.offset, result.err
	case <-timeout.C:
		log.Printf("[PebbleStorage] Batch processing timeout for %s:%d", topicName, partition)
		ps.metrics.IncrementProduceErrors()
		return -1, fmt.Errorf("batch processing timeout after 30s")
	}
}

// batchWorker processes batches of writes for high throughput
func (ps *PebbleStorage) batchWorker(workerID int) {
	defer ps.batchWG.Done()

	batch := ps.db.NewBatch()
	defer batch.Close()

	writes := make([]*batchWrite, 0, ps.batchSize)
	timer := time.NewTimer(ps.batchTimeout)
	
	// Track worker performance
	processedBatches := 0

	for {
		select {
		case write, ok := <-ps.pendingWrites:
			if !ok {
				// Channel closed, process remaining batch
				if len(writes) > 0 {
					ps.processBatch(writes, batch, workerID)
				}
				return
			}

			writes = append(writes, write)

			// Flush batch if it's full
			if len(writes) >= ps.batchSize {
				ps.processBatch(writes, batch, workerID)
				writes = writes[:0]
				batch = ps.db.NewBatch()
				timer.Reset(ps.batchTimeout)
				processedBatches++
			}

		case <-timer.C:
			// Timeout reached, flush current batch
			if len(writes) > 0 {
				ps.processBatch(writes, batch, workerID)
				writes = writes[:0]
				batch = ps.db.NewBatch()
				processedBatches++
			}
			timer.Reset(ps.batchTimeout)

		case <-ps.stopBatching:
			// Process remaining batch before stopping
			if len(writes) > 0 {
				ps.processBatch(writes, batch, workerID)
				processedBatches++
			}
			log.Printf("[PebbleStorage] Worker %d processed %d batches", workerID, processedBatches)
			return
		}
	}
}

// processBatch processes a batch of writes atomically with optimized offset allocation
func (ps *PebbleStorage) processBatch(writes []*batchWrite, batch *pebble.Batch, workerID int) {
	start := time.Now()
	defer func() {
		ps.metrics.RecordBatchProcessLatency(time.Since(start), len(writes))
	}()
	
	// Reset the batch
	batch.Reset()
	
	// Group writes by topic:partition for efficient offset allocation
	partitionGroups := make(map[string][]*batchWrite)
	for _, write := range writes {
		key := fmt.Sprintf("%s:%d", write.topicName, write.partition)
		partitionGroups[key] = append(partitionGroups[key], write)
	}
	
	if len(partitionGroups) == 0 {
		return
	}
	
	log.Printf("[PebbleStorage] Worker %d processing batch with %d writes across %d partitions", workerID, len(writes), len(partitionGroups))
	
	// Process each partition group with per-partition locking
	for partitionKey, partitionWrites := range partitionGroups {
		if len(partitionWrites) == 0 {
			continue
		}
		
		firstWrite := partitionWrites[0]
		
		// Get or create partition mutex for fine-grained locking
		mutexInterface, _ := ps.partitionMutexes.LoadOrStore(partitionKey, &sync.Mutex{})
		partitionMutex := mutexInterface.(*sync.Mutex)
		
		// Lock this partition for offset allocation
		partitionMutex.Lock()
		
		// Get the starting offset for this partition (optimized with caching)
		startOffset, err := ps.getNextOffsetCached(firstWrite.topicName, firstWrite.partition, partitionKey)
		if err != nil {
			log.Printf("[PebbleStorage] Worker %d failed to get offset for %s: %v", workerID, partitionKey, err)
			ps.metrics.IncrementBatchErrors()
			// Mark all writes in this partition as failed with detailed error
			for _, write := range partitionWrites {
				select {
				case write.resultCh <- batchResult{-1, fmt.Errorf("failed to get next offset for %s:%d: %v", firstWrite.topicName, firstWrite.partition, err)}:
				default:
					log.Printf("[PebbleStorage] Failed to send error result to write channel")
				}
			}
			partitionMutex.Unlock()
			continue
		}
		
		// Update cached offset for this partition
		newOffset := startOffset + int64(len(partitionWrites))
		ps.offsetCache.Store(partitionKey, &newOffset)
		partitionMutex.Unlock()
		
		// Process all writes in this partition sequentially (batch serialization)
		timestamp := time.Now() // Use same timestamp for all messages in batch
		for i, write := range partitionWrites {
			offset := startOffset + int64(i)
			
			// Create the key: topic:partition:offset
			key := makeMessageKey(write.topicName, write.partition, offset)
			
			// Serialize the message with timestamp
			messageData := &PersistedMessage{
				Key:       write.msg.Key,
				Value:     write.msg.Value,
				Timestamp: timestamp,
				Offset:    offset,
			}
			
			value, err := json.Marshal(messageData)
			if err != nil {
				log.Printf("[PebbleStorage] JSON marshal failed for message %s:%d at offset %d: %v", write.topicName, write.partition, offset, err)
				ps.metrics.IncrementBatchErrors()
				select {
				case write.resultCh <- batchResult{-1, fmt.Errorf("failed to serialize message: %v", err)}:
				default:
					log.Printf("[PebbleStorage] Failed to send serialization error to result channel")
				}
				continue
			}

			// Add to batch (no sync for better performance)
			if err := batch.Set(key, value, nil); err != nil {
				log.Printf("[PebbleStorage] Batch.Set failed for %s:%d at offset %d: %v", write.topicName, write.partition, offset, err)
				ps.metrics.IncrementBatchErrors()
				select {
				case write.resultCh <- batchResult{-1, fmt.Errorf("failed to add message to batch: %v", err)}:
				default:
					log.Printf("[PebbleStorage] Failed to send batch set error to result channel")
				}
				continue
			}

			// Mark as successful - will be confirmed after batch commit
			write.resultCh <- batchResult{offset, nil}
		}
		
		// Update the final offset for this partition (single update per partition)
		finalOffset := startOffset + int64(len(partitionWrites))
		offsetKey := makeOffsetKey(firstWrite.topicName, firstWrite.partition)
		offsetValue := make([]byte, 8)
		binary.BigEndian.PutUint64(offsetValue, uint64(finalOffset))
		if err := batch.Set(offsetKey, offsetValue, nil); err != nil {
			log.Printf("[PebbleStorage] Failed to update offset for %s: %v", partitionKey, err)
			ps.metrics.IncrementBatchErrors()
			// If offset update fails, mark all writes as failed with detailed error
			for _, write := range partitionWrites {
				select {
				case write.resultCh <- batchResult{-1, fmt.Errorf("failed to update partition offset for %s:%d: %v", firstWrite.topicName, firstWrite.partition, err)}:
				default:
					log.Printf("[PebbleStorage] Failed to send offset update error to result channel")
				}
			}
		}
	}

	// Commit the entire batch with async write (no sync for better throughput)
	commitStart := time.Now()
	if err := batch.Commit(nil); err != nil {
		commitDuration := time.Since(commitStart)
		log.Printf("[PebbleStorage] Worker %d batch commit failed after %v with %d writes across %d partitions: %v", workerID, commitDuration, len(writes), len(partitionGroups), err)
		ps.metrics.IncrementBatchErrors()
		
		// If batch commit fails, update all results with error and clear offset cache
		for partitionKey := range partitionGroups {
			ps.offsetCache.Delete(partitionKey) // Clear invalid cached offsets
		}
		
		for _, write := range writes {
			select {
			case write.resultCh <- batchResult{-1, fmt.Errorf("batch commit failed after %v: %v", commitDuration, err)}:
			default:
				log.Printf("[PebbleStorage] Failed to send batch commit error to result channel")
			}
		}
		return // Early return on commit failure
	}
	
	commitDuration := time.Since(commitStart)
	log.Printf("[PebbleStorage] Worker %d successfully committed batch with %d writes in %v", workerID, len(writes), commitDuration)
}

// Read reads messages from a partition starting at the given offset
func (ps *PebbleStorage) Read(topicName string, partition int32, offset int64, maxMessages int32) ([]*Message, bool, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if ps.closed {
		return nil, false, fmt.Errorf("storage is closed")
	}

	var messages []*Message
	count := int32(0)
	
	// Create iterator for the partition range
	startKey := makeMessageKey(topicName, partition, offset)
	endKey := makeMessageKey(topicName, partition+1, 0) // Next partition start
	
	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		return nil, false, fmt.Errorf("failed to create iterator: %v", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid() && count < maxMessages; iter.Next() {
		var msg PersistedMessage
		if err := json.Unmarshal(iter.Value(), &msg); err != nil {
			continue // Skip corrupted messages
		}

		messages = append(messages, &Message{
			Key:   msg.Key,
			Value: msg.Value,
		})
		count++
	}

	if err := iter.Error(); err != nil {
		return nil, false, fmt.Errorf("iterator error: %v", err)
	}

	// Check if there are more messages
	hasMore := false
	if iter.Valid() {
		hasMore = true
	}

	return messages, hasMore, nil
}

// GetNextOffset returns the next offset for a partition
func (ps *PebbleStorage) GetNextOffset(topicName string, partition int32) (int64, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if ps.closed {
		return -1, fmt.Errorf("storage is closed")
	}

	return ps.getNextOffset(topicName, partition)
}

// getNextOffset internal method to get next offset (assumes lock held)
func (ps *PebbleStorage) getNextOffset(topicName string, partition int32) (int64, error) {
	key := makeOffsetKey(topicName, partition)
	
	value, closer, err := ps.db.Get(key)
	if err == pebble.ErrNotFound {
		return 0, nil // First message
	}
	if err != nil {
		return -1, fmt.Errorf("failed to get offset: %v", err)
	}
	defer closer.Close()

	if len(value) < 8 {
		return 0, nil // Corrupted data, start from 0
	}

	return int64(binary.BigEndian.Uint64(value)), nil
}

// getNextOffsetCached optimized version that uses in-memory caching for batch processing
func (ps *PebbleStorage) getNextOffsetCached(topicName string, partition int32, partitionKey string) (int64, error) {
	// First check cache
	if cachedOffset, exists := ps.offsetCache.Load(partitionKey); exists {
		offset := *cachedOffset.(*int64)
		log.Printf("[PebbleStorage] Using cached offset %d for %s", offset, partitionKey)
		return offset, nil
	}
	
	// Cache miss - fetch from storage
	offset, err := ps.getNextOffset(topicName, partition)
	if err != nil {
		return -1, err
	}
	
	// Cache the result
	ps.offsetCache.Store(partitionKey, &offset)
	log.Printf("[PebbleStorage] Cached offset %d for %s", offset, partitionKey)
	
	return offset, nil
}

// GetPartitionSize returns the number of messages in a partition
func (ps *PebbleStorage) GetPartitionSize(topicName string, partition int32) (int64, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if ps.closed {
		return -1, fmt.Errorf("storage is closed")
	}

	count := int64(0)
	startKey := makeMessageKey(topicName, partition, 0)
	endKey := makeMessageKey(topicName, partition+1, 0)

	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		return -1, fmt.Errorf("failed to create iterator: %v", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	return count, iter.Error()
}

// PersistedMessage represents a message stored in Pebble
type PersistedMessage struct {
	Key       string    `json:"key"`
	Value     []byte    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	Offset    int64     `json:"offset"`
}

// Key format: "msg:{topic}:{partition}:{offset}"
func makeMessageKey(topic string, partition int32, offset int64) []byte {
	key := fmt.Sprintf("msg:%s:%d:%d", topic, partition, offset)
	return []byte(key)
}

// Offset key format: "offset:{topic}:{partition}"
func makeOffsetKey(topic string, partition int32) []byte {
	key := fmt.Sprintf("offset:%s:%d", topic, partition)
	return []byte(key)
}

// GetMetrics returns performance metrics
func (ps *PebbleStorage) GetMetrics() *metrics.PerformanceMetrics {
	ps.metrics.CalculateRates()
	return ps.metrics.GetSnapshot()
}