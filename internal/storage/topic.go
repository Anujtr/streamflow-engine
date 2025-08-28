package storage

import (
	"fmt"
	"hash/fnv"
	"sync"
)

type Topic struct {
	Name       string
	Partitions map[int32]*Partition
	NumPartitions int32
	mu         sync.RWMutex
}

func NewTopic(name string, numPartitions int32) *Topic {
	if numPartitions <= 0 {
		numPartitions = 1
	}

	partitions := make(map[int32]*Partition)
	for i := int32(0); i < numPartitions; i++ {
		partitions[i] = NewPartition(i)
	}

	return &Topic{
		Name:          name,
		Partitions:    partitions,
		NumPartitions: numPartitions,
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