package persistence

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestOffsetStore_CommitAndGetOffset(t *testing.T) {
	tmpDir := t.TempDir()
	
	// Create Pebble database
	db, err := pebble.Open(tmpDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("Failed to open Pebble database: %v", err)
	}
	defer db.Close()

	offsetStore := NewOffsetStore(db)

	consumerGroup := "test-group"
	topic := "test-topic"
	partition := int32(0)
	offset := int64(100)

	// Test committing offset
	err = offsetStore.CommitOffset(consumerGroup, topic, partition, offset)
	if err != nil {
		t.Fatalf("Failed to commit offset: %v", err)
	}

	// Test getting offset
	retrievedOffset, err := offsetStore.GetOffset(consumerGroup, topic, partition)
	if err != nil {
		t.Fatalf("Failed to get offset: %v", err)
	}

	if retrievedOffset != offset {
		t.Errorf("Expected offset %d, got %d", offset, retrievedOffset)
	}
}

func TestOffsetStore_GetOffsetNotExist(t *testing.T) {
	tmpDir := t.TempDir()
	
	db, err := pebble.Open(tmpDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("Failed to open Pebble database: %v", err)
	}
	defer db.Close()

	offsetStore := NewOffsetStore(db)

	// Test getting non-existent offset
	offset, err := offsetStore.GetOffset("non-existent", "topic", 0)
	if err != nil {
		t.Fatalf("Failed to get non-existent offset: %v", err)
	}

	if offset != 0 {
		t.Errorf("Expected offset 0 for non-existent key, got %d", offset)
	}
}

func TestOffsetStore_MultipleConsumerGroups(t *testing.T) {
	tmpDir := t.TempDir()
	
	db, err := pebble.Open(tmpDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("Failed to open Pebble database: %v", err)
	}
	defer db.Close()

	offsetStore := NewOffsetStore(db)

	topic := "test-topic"
	partition := int32(0)

	// Commit offsets for different consumer groups
	groups := []string{"group-1", "group-2", "group-3"}
	offsets := []int64{100, 200, 300}

	for i, group := range groups {
		err = offsetStore.CommitOffset(group, topic, partition, offsets[i])
		if err != nil {
			t.Fatalf("Failed to commit offset for group %s: %v", group, err)
		}
	}

	// Verify each group has its own offset
	for i, group := range groups {
		retrievedOffset, err := offsetStore.GetOffset(group, topic, partition)
		if err != nil {
			t.Fatalf("Failed to get offset for group %s: %v", group, err)
		}

		if retrievedOffset != offsets[i] {
			t.Errorf("Expected offset %d for group %s, got %d", offsets[i], group, retrievedOffset)
		}
	}
}

func TestOffsetStore_MultiplePartitions(t *testing.T) {
	tmpDir := t.TempDir()
	
	db, err := pebble.Open(tmpDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("Failed to open Pebble database: %v", err)
	}
	defer db.Close()

	offsetStore := NewOffsetStore(db)

	consumerGroup := "test-group"
	topic := "test-topic"
	numPartitions := 4

	// Commit offsets for different partitions
	for i := 0; i < numPartitions; i++ {
		partition := int32(i)
		offset := int64(i * 100) // Different offset per partition
		
		err = offsetStore.CommitOffset(consumerGroup, topic, partition, offset)
		if err != nil {
			t.Fatalf("Failed to commit offset for partition %d: %v", partition, err)
		}
	}

	// Verify each partition has its own offset
	for i := 0; i < numPartitions; i++ {
		partition := int32(i)
		expectedOffset := int64(i * 100)
		
		retrievedOffset, err := offsetStore.GetOffset(consumerGroup, topic, partition)
		if err != nil {
			t.Fatalf("Failed to get offset for partition %d: %v", partition, err)
		}

		if retrievedOffset != expectedOffset {
			t.Errorf("Expected offset %d for partition %d, got %d", expectedOffset, partition, retrievedOffset)
		}
	}
}

func TestOffsetStore_GetAllOffsets(t *testing.T) {
	tmpDir := t.TempDir()
	
	db, err := pebble.Open(tmpDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("Failed to open Pebble database: %v", err)
	}
	defer db.Close()

	offsetStore := NewOffsetStore(db)

	consumerGroup := "test-group"
	topic := "test-topic"
	numPartitions := 3

	expectedOffsets := make(map[int32]int64)

	// Commit offsets for multiple partitions
	for i := 0; i < numPartitions; i++ {
		partition := int32(i)
		offset := int64(i*100 + 50)
		expectedOffsets[partition] = offset
		
		err = offsetStore.CommitOffset(consumerGroup, topic, partition, offset)
		if err != nil {
			t.Fatalf("Failed to commit offset for partition %d: %v", partition, err)
		}
	}

	// Get all offsets
	allOffsets, err := offsetStore.GetAllOffsets(consumerGroup, topic)
	if err != nil {
		t.Fatalf("Failed to get all offsets: %v", err)
	}

	if len(allOffsets) != numPartitions {
		t.Errorf("Expected %d offsets, got %d", numPartitions, len(allOffsets))
	}

	for partition, expectedOffset := range expectedOffsets {
		if retrievedOffset, exists := allOffsets[partition]; !exists {
			t.Errorf("Missing offset for partition %d", partition)
		} else if retrievedOffset != expectedOffset {
			t.Errorf("Expected offset %d for partition %d, got %d", expectedOffset, partition, retrievedOffset)
		}
	}
}

func TestOffsetStore_DeleteConsumerGroup(t *testing.T) {
	tmpDir := t.TempDir()
	
	db, err := pebble.Open(tmpDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("Failed to open Pebble database: %v", err)
	}
	defer db.Close()

	offsetStore := NewOffsetStore(db)

	consumerGroup := "test-group"
	topic := "test-topic"
	numPartitions := 3

	// Commit offsets for multiple partitions
	for i := 0; i < numPartitions; i++ {
		partition := int32(i)
		offset := int64(i * 100)
		
		err = offsetStore.CommitOffset(consumerGroup, topic, partition, offset)
		if err != nil {
			t.Fatalf("Failed to commit offset for partition %d: %v", partition, err)
		}
	}

	// Verify offsets exist
	allOffsets, err := offsetStore.GetAllOffsets(consumerGroup, topic)
	if err != nil {
		t.Fatalf("Failed to get all offsets before delete: %v", err)
	}
	if len(allOffsets) != numPartitions {
		t.Errorf("Expected %d offsets before delete, got %d", numPartitions, len(allOffsets))
	}

	// Delete consumer group
	err = offsetStore.DeleteConsumerGroup(consumerGroup)
	if err != nil {
		t.Fatalf("Failed to delete consumer group: %v", err)
	}

	// Verify offsets are gone
	allOffsets, err = offsetStore.GetAllOffsets(consumerGroup, topic)
	if err != nil {
		t.Fatalf("Failed to get all offsets after delete: %v", err)
	}
	if len(allOffsets) != 0 {
		t.Errorf("Expected 0 offsets after delete, got %d", len(allOffsets))
	}

	// Verify individual gets return 0
	for i := 0; i < numPartitions; i++ {
		partition := int32(i)
		
		retrievedOffset, err := offsetStore.GetOffset(consumerGroup, topic, partition)
		if err != nil {
			t.Fatalf("Failed to get offset for deleted partition %d: %v", partition, err)
		}
		if retrievedOffset != 0 {
			t.Errorf("Expected offset 0 for deleted partition %d, got %d", partition, retrievedOffset)
		}
	}
}