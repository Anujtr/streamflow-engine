package storage

import (
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/Anujtr/streamflow-engine/internal/persistence"
)

// PartitionInterface defines the interface that both in-memory and persistent partitions implement
type PartitionInterface interface {
	Append(msg *Message) int64
	Read(offset int64, maxMessages int32) ([]*Message, bool)
	Size() int
	GetNextOffset() int64
	GetID() int32
}

type Topic struct {
	Name       string
	Partitions map[int32]PartitionInterface
	NumPartitions int32
	mu         sync.RWMutex
	persistenceMode bool
	storage    *persistence.PebbleStorage
}

func NewTopic(name string, numPartitions int32) *Topic {
	if numPartitions <= 0 {
		numPartitions = 1
	}

	partitions := make(map[int32]PartitionInterface)
	for i := int32(0); i < numPartitions; i++ {
		partitions[i] = NewPartition(i)
	}

	return &Topic{
		Name:            name,
		Partitions:      partitions,
		NumPartitions:   numPartitions,
		persistenceMode: false,
	}
}

// NewPersistentTopic creates a new topic with persistent partitions
func NewPersistentTopic(name string, numPartitions int32, storage *persistence.PebbleStorage) *Topic {
	if numPartitions <= 0 {
		numPartitions = 1
	}

	partitions := make(map[int32]PartitionInterface)
	for i := int32(0); i < numPartitions; i++ {
		partitions[i] = NewPersistentPartition(i, name, storage)
	}

	return &Topic{
		Name:            name,
		Partitions:      partitions,
		NumPartitions:   numPartitions,
		persistenceMode: true,
		storage:         storage,
	}
}

func (t *Topic) GetPartition(key string) int32 {
	if key == "" {
		return 0
	}

	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	hash := hasher.Sum32()
	
	// Ensure positive result for modulo operation
	partition := int32(hash % uint32(t.NumPartitions))
	if partition < 0 {
		partition = -partition
	}
	
	return partition
}

// AddPartition adds a new partition to the topic
func (t *Topic) AddPartition(partitionID int32) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.Partitions[partitionID]; exists {
		return fmt.Errorf("partition %d already exists", partitionID)
	}

	if t.persistenceMode && t.storage != nil {
		t.Partitions[partitionID] = NewPersistentPartition(partitionID, t.Name, t.storage)
	} else {
		t.Partitions[partitionID] = NewPartition(partitionID)
	}
	
	if partitionID >= t.NumPartitions {
		t.NumPartitions = partitionID + 1
	}

	return nil
}

// RemovePartition removes a partition from the topic (careful: data loss)
func (t *Topic) RemovePartition(partitionID int32) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.Partitions[partitionID]; !exists {
		return fmt.Errorf("partition %d does not exist", partitionID)
	}

	delete(t.Partitions, partitionID)
	return nil
}

func (t *Topic) Produce(msg *Message) (int32, int64, error) {
	partition := t.GetPartition(msg.Key)
	
	t.mu.RLock()
	p, exists := t.Partitions[partition]
	t.mu.RUnlock()
	
	if !exists {
		return -1, -1, fmt.Errorf("partition %d does not exist", partition)
	}

	offset := p.Append(msg)
	return partition, offset, nil
}

func (t *Topic) Consume(partition int32, offset int64, maxMessages int32) ([]*Message, bool, error) {
	t.mu.RLock()
	p, exists := t.Partitions[partition]
	t.mu.RUnlock()
	
	if !exists {
		return nil, false, fmt.Errorf("partition %d does not exist", partition)
	}

	messages, hasMore := p.Read(offset, maxMessages)
	return messages, hasMore, nil
}

func (t *Topic) GetPartitionInfo() map[int32]PartitionInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	info := make(map[int32]PartitionInfo)
	for id, partition := range t.Partitions {
		info[id] = PartitionInfo{
			ID:         id,
			Size:       int64(partition.Size()),
			NextOffset: partition.GetNextOffset(),
		}
	}
	return info
}

type PartitionInfo struct {
	ID         int32
	Size       int64
	NextOffset int64
}