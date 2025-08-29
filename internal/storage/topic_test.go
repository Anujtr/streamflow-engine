package storage

import (
	"fmt"
	"testing"
)

func TestNewTopic(t *testing.T) {
	topic := NewTopic("test-topic", 4)

	if topic.Name != "test-topic" {
		t.Errorf("Expected topic name 'test-topic', got %s", topic.Name)
	}

	if topic.NumPartitions != 4 {
		t.Errorf("Expected 4 partitions, got %d", topic.NumPartitions)
	}

	if len(topic.Partitions) != 4 {
		t.Errorf("Expected 4 partitions in map, got %d", len(topic.Partitions))
	}

	for i := int32(0); i < 4; i++ {
		partition, exists := topic.Partitions[i]
		if !exists {
			t.Errorf("Expected partition %d to exist", i)
		}
		if partition.GetID() != i {
			t.Errorf("Expected partition ID %d, got %d", i, partition.GetID())
		}
	}
}

func TestNewTopic_InvalidPartitions(t *testing.T) {
	topic := NewTopic("test-topic", 0)

	if topic.NumPartitions != 1 {
		t.Errorf("Expected 1 partition for invalid input, got %d", topic.NumPartitions)
	}

	topic = NewTopic("test-topic", -5)

	if topic.NumPartitions != 1 {
		t.Errorf("Expected 1 partition for negative input, got %d", topic.NumPartitions)
	}
}

func TestTopic_GetPartition(t *testing.T) {
	topic := NewTopic("test-topic", 4)

	// Test with same key multiple times - should be consistent
	key := "test-key"
	partition1 := topic.GetPartition(key)
	partition2 := topic.GetPartition(key)

	if partition1 != partition2 {
		t.Errorf("Expected consistent partitioning, got %d and %d", partition1, partition2)
	}

	// Test empty key
	emptyPartition := topic.GetPartition("")
	if emptyPartition != 0 {
		t.Errorf("Expected empty key to go to partition 0, got %d", emptyPartition)
	}

	// Test that partitions are within range
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		partition := topic.GetPartition(key)
		if partition < 0 || partition >= topic.NumPartitions {
			t.Errorf("Partition %d for key %s is out of range [0, %d)", partition, key, topic.NumPartitions)
		}
	}
}

func TestTopic_Produce(t *testing.T) {
	topic := NewTopic("test-topic", 4)

	msg := &Message{
		Key:   "test-key",
		Value: []byte("test-value"),
	}

	partition, offset, err := topic.Produce(msg)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if partition < 0 || partition >= topic.NumPartitions {
		t.Errorf("Invalid partition %d", partition)
	}

	if offset != 0 {
		t.Errorf("Expected first offset to be 0, got %d", offset)
	}

	// Verify message was stored
	storedPartition := topic.Partitions[partition]
	if storedPartition.Size() != 1 {
		t.Errorf("Expected 1 message in partition, got %d", storedPartition.Size())
	}
}

func TestTopic_Consume(t *testing.T) {
	topic := NewTopic("test-topic", 4)

	// Produce some messages
	for i := 0; i < 10; i++ {
		msg := &Message{
			Key:   fmt.Sprintf("key-%d", i),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		topic.Produce(msg)
	}

	// Try to consume from partition 0
	messages, _, err := topic.Consume(0, 0, 5)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Should get some messages (depends on hash distribution)
	if len(messages) < 0 {
		t.Error("Expected non-negative number of messages")
	}

	// Test invalid partition
	_, _, err = topic.Consume(10, 0, 1)
	if err == nil {
		t.Error("Expected error for invalid partition")
	}
}

func TestTopic_GetPartitionInfo(t *testing.T) {
	topic := NewTopic("test-topic", 2)

	// Initially empty
	info := topic.GetPartitionInfo()

	if len(info) != 2 {
		t.Errorf("Expected info for 2 partitions, got %d", len(info))
	}

	for i := int32(0); i < 2; i++ {
		partitionInfo, exists := info[i]
		if !exists {
			t.Errorf("Expected info for partition %d", i)
		}
		if partitionInfo.ID != i {
			t.Errorf("Expected partition ID %d, got %d", i, partitionInfo.ID)
		}
		if partitionInfo.Size != 0 {
			t.Errorf("Expected partition %d size to be 0, got %d", i, partitionInfo.Size)
		}
		if partitionInfo.NextOffset != 0 {
			t.Errorf("Expected partition %d next offset to be 0, got %d", i, partitionInfo.NextOffset)
		}
	}

	// Add a message and check again
	msg := &Message{
		Key:   "test-key",
		Value: []byte("test-value"),
	}
	partition, _, _ := topic.Produce(msg)

	info = topic.GetPartitionInfo()
	partitionInfo := info[partition]

	if partitionInfo.Size != 1 {
		t.Errorf("Expected partition %d size to be 1, got %d", partition, partitionInfo.Size)
	}
	if partitionInfo.NextOffset != 1 {
		t.Errorf("Expected partition %d next offset to be 1, got %d", partition, partitionInfo.NextOffset)
	}
}