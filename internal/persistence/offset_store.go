package persistence

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
)

// OffsetStore manages consumer group offsets using Pebble
type OffsetStore struct {
	db *pebble.DB
	mu sync.RWMutex
}

// NewOffsetStore creates a new offset store
func NewOffsetStore(db *pebble.DB) *OffsetStore {
	return &OffsetStore{
		db: db,
	}
}

// CommitOffset commits the offset for a consumer group, topic, and partition
func (os *OffsetStore) CommitOffset(consumerGroup, topic string, partition int32, offset int64) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	key := makeConsumerOffsetKey(consumerGroup, topic, partition)
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(offset))

	return os.db.Set(key, value, pebble.Sync)
}

// GetOffset retrieves the committed offset for a consumer group, topic, and partition
func (os *OffsetStore) GetOffset(consumerGroup, topic string, partition int32) (int64, error) {
	os.mu.RLock()
	defer os.mu.RUnlock()

	key := makeConsumerOffsetKey(consumerGroup, topic, partition)
	
	value, closer, err := os.db.Get(key)
	if err == pebble.ErrNotFound {
		return 0, nil // Start from beginning if no offset is committed
	}
	if err != nil {
		return -1, fmt.Errorf("failed to get consumer offset: %v", err)
	}
	defer closer.Close()

	if len(value) < 8 {
		return 0, nil // Corrupted data, start from 0
	}

	return int64(binary.BigEndian.Uint64(value)), nil
}

// GetAllOffsets returns all committed offsets for a consumer group and topic
func (os *OffsetStore) GetAllOffsets(consumerGroup, topic string) (map[int32]int64, error) {
	os.mu.RLock()
	defer os.mu.RUnlock()

	offsets := make(map[int32]int64)
	prefix := fmt.Sprintf("consumer_offset:%s:%s:", consumerGroup, topic)
	
	iter, err := os.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "~"), // ASCII ~ is greater than all partition numbers
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %v", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		
		// Parse partition from key: "consumer_offset:{group}:{topic}:{partition}"
		var partition int32
		if n, err := fmt.Sscanf(key, "consumer_offset:%s:%s:%d", &consumerGroup, &topic, &partition); n == 3 && err == nil {
			if len(iter.Value()) >= 8 {
				offset := int64(binary.BigEndian.Uint64(iter.Value()))
				offsets[partition] = offset
			}
		}
	}

	return offsets, iter.Error()
}

// DeleteConsumerGroup removes all offsets for a consumer group
func (os *OffsetStore) DeleteConsumerGroup(consumerGroup string) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	prefix := fmt.Sprintf("consumer_offset:%s:", consumerGroup)
	
	iter, err := os.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "~"),
	})
	if err != nil {
		return fmt.Errorf("failed to create iterator: %v", err)
	}
	defer iter.Close()

	batch := os.db.NewBatch()
	defer batch.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key(), nil); err != nil {
			return fmt.Errorf("failed to delete key: %v", err)
		}
	}

	return batch.Commit(pebble.Sync)
}

// Consumer offset key format: "consumer_offset:{group}:{topic}:{partition}"
func makeConsumerOffsetKey(consumerGroup, topic string, partition int32) []byte {
	key := fmt.Sprintf("consumer_offset:%s:%s:%d", consumerGroup, topic, partition)
	return []byte(key)
}