package persistence

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

// PebbleStorage provides persistent storage using Pebble database
type PebbleStorage struct {
	db         *pebble.DB
	dataDir    string
	mu         sync.RWMutex
	closed     bool
}

// NewPebbleStorage creates a new Pebble-based storage instance
func NewPebbleStorage(dataDir string) (*PebbleStorage, error) {
	// Create data directory if it doesn't exist
	dbPath := filepath.Join(dataDir, "pebble-db")
	
	// Configure Pebble options for high performance
	opts := &pebble.Options{
		Cache:                 pebble.NewCache(128 << 20), // 128MB cache
		MemTableSize:          64 << 20,                   // 64MB memtable
		MaxConcurrentCompactions: func() int { return 4 },
		L0StopWritesThreshold:    32,
		LBaseMaxBytes:            64 << 20, // 64MB
	}

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open Pebble database: %v", err)
	}

	return &PebbleStorage{
		db:      db,
		dataDir: dataDir,
	}, nil
}

// Close closes the Pebble database
func (ps *PebbleStorage) Close() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.closed {
		return nil
	}

	ps.closed = true
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
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if ps.closed {
		return -1, fmt.Errorf("storage is closed")
	}

	// Get next offset for this partition
	offset, err := ps.getNextOffset(topicName, partition)
	if err != nil {
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