package storage

import (
	"sync"

	"github.com/Anujtr/streamflow-engine/internal/persistence"
)

// PersistentPartition represents a partition backed by Pebble storage
type PersistentPartition struct {
	ID      int32
	storage *persistence.PebbleStorage
	topic   string
	mu      sync.RWMutex
}

// NewPersistentPartition creates a new persistent partition
func NewPersistentPartition(id int32, topic string, storage *persistence.PebbleStorage) *PersistentPartition {
	return &PersistentPartition{
		ID:      id,
		storage: storage,
		topic:   topic,
	}
}

// Append appends a message to the partition and returns the assigned offset
func (pp *PersistentPartition) Append(msg *Message) int64 {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	// Convert storage.Message to persistence.Message
	persistentMsg := &persistence.Message{
		Key:   msg.Key,
		Value: msg.Value,
	}

	offset, err := pp.storage.Append(pp.topic, pp.ID, persistentMsg)
	if err != nil {
		// For now, return -1 on error to maintain compatibility
		// In a full implementation, we'd want to handle this more gracefully
		return -1
	}

	return offset
}

// Read reads messages from the partition starting at the given offset
func (pp *PersistentPartition) Read(offset int64, maxMessages int32) ([]*Message, bool) {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	persistentMessages, hasMore, err := pp.storage.Read(pp.topic, pp.ID, offset, maxMessages)
	if err != nil {
		// Return empty result on error
		return []*Message{}, false
	}

	// Convert persistence.Message to storage.Message
	messages := make([]*Message, len(persistentMessages))
	for i, pmsg := range persistentMessages {
		messages[i] = &Message{
			Key:   pmsg.Key,
			Value: pmsg.Value,
		}
	}

	return messages, hasMore
}

// Size returns the number of messages in the partition
func (pp *PersistentPartition) Size() int {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	size, err := pp.storage.GetPartitionSize(pp.topic, pp.ID)
	if err != nil {
		return 0
	}

	return int(size)
}

// GetNextOffset returns the next offset for the partition
func (pp *PersistentPartition) GetNextOffset() int64 {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	offset, err := pp.storage.GetNextOffset(pp.topic, pp.ID)
	if err != nil {
		return 0
	}

	return offset
}