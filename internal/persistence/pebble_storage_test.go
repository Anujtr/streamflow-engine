package persistence

import (
	"testing"
)

func TestPebbleStorage_Basic(t *testing.T) {
	// Create temporary directory for testing
	tmpDir := t.TempDir()
	
	storage, err := NewPebbleStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create Pebble storage: %v", err)
	}
	defer storage.Close()

	// Test basic append and read
	topic := "test-topic"
	partition := int32(0)
	
	msg := &Message{
		Key:   "test-key",
		Value: []byte("test-value"),
	}

	// Append message
	offset, err := storage.Append(topic, partition, msg)
	if err != nil {
		t.Fatalf("Failed to append message: %v", err)
	}
	
	if offset != 0 {
		t.Errorf("Expected offset 0, got %d", offset)
	}

	// Read messages
	messages, hasMore, err := storage.Read(topic, partition, 0, 10)
	if err != nil {
		t.Fatalf("Failed to read messages: %v", err)
	}

	if len(messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(messages))
	}

	if hasMore {
		t.Error("Expected hasMore to be false")
	}

	if messages[0].Key != msg.Key {
		t.Errorf("Expected key %s, got %s", msg.Key, messages[0].Key)
	}

	if string(messages[0].Value) != string(msg.Value) {
		t.Errorf("Expected value %s, got %s", string(msg.Value), string(messages[0].Value))
	}
}

func TestPebbleStorage_MultipleMessages(t *testing.T) {
	tmpDir := t.TempDir()
	
	storage, err := NewPebbleStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create Pebble storage: %v", err)
	}
	defer storage.Close()

	topic := "test-topic"
	partition := int32(0)
	numMessages := 100

	// Append multiple messages
	for i := 0; i < numMessages; i++ {
		msg := &Message{
			Key:   "key-" + string(rune(i)),
			Value: []byte("value-" + string(rune(i))),
		}

		offset, err := storage.Append(topic, partition, msg)
		if err != nil {
			t.Fatalf("Failed to append message %d: %v", i, err)
		}

		if offset != int64(i) {
			t.Errorf("Expected offset %d, got %d", i, offset)
		}
	}

	// Read all messages
	messages, hasMore, err := storage.Read(topic, partition, 0, int32(numMessages))
	if err != nil {
		t.Fatalf("Failed to read messages: %v", err)
	}

	if len(messages) != numMessages {
		t.Errorf("Expected %d messages, got %d", numMessages, len(messages))
	}

	if hasMore {
		t.Error("Expected hasMore to be false")
	}

	// Test partial read
	messages, hasMore, err = storage.Read(topic, partition, 0, 10)
	if err != nil {
		t.Fatalf("Failed to read partial messages: %v", err)
	}

	if len(messages) != 10 {
		t.Errorf("Expected 10 messages, got %d", len(messages))
	}

	if !hasMore {
		t.Error("Expected hasMore to be true")
	}
}

func TestPebbleStorage_MultiplePartitions(t *testing.T) {
	tmpDir := t.TempDir()
	
	storage, err := NewPebbleStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create Pebble storage: %v", err)
	}
	defer storage.Close()

	topic := "test-topic"
	numPartitions := int32(4)
	messagesPerPartition := 10

	// Append messages to different partitions
	for partition := int32(0); partition < numPartitions; partition++ {
		for i := 0; i < messagesPerPartition; i++ {
			msg := &Message{
				Key:   "key-" + string(rune(i)),
				Value: []byte("partition-" + string(rune(partition)) + "-value-" + string(rune(i))),
			}

			offset, err := storage.Append(topic, partition, msg)
			if err != nil {
				t.Fatalf("Failed to append message to partition %d: %v", partition, err)
			}

			if offset != int64(i) {
				t.Errorf("Expected offset %d for partition %d, got %d", i, partition, offset)
			}
		}
	}

	// Read from each partition
	for partition := int32(0); partition < numPartitions; partition++ {
		messages, hasMore, err := storage.Read(topic, partition, 0, int32(messagesPerPartition))
		if err != nil {
			t.Fatalf("Failed to read from partition %d: %v", partition, err)
		}

		if len(messages) != messagesPerPartition {
			t.Errorf("Expected %d messages from partition %d, got %d", messagesPerPartition, partition, len(messages))
		}

		if hasMore {
			t.Errorf("Expected hasMore to be false for partition %d", partition)
		}
	}
}

func TestPebbleStorage_GetNextOffset(t *testing.T) {
	tmpDir := t.TempDir()
	
	storage, err := NewPebbleStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create Pebble storage: %v", err)
	}
	defer storage.Close()

	topic := "test-topic"
	partition := int32(0)

	// Initially should be 0
	offset, err := storage.GetNextOffset(topic, partition)
	if err != nil {
		t.Fatalf("Failed to get next offset: %v", err)
	}
	if offset != 0 {
		t.Errorf("Expected initial offset 0, got %d", offset)
	}

	// Add a message
	msg := &Message{Key: "test", Value: []byte("test")}
	_, err = storage.Append(topic, partition, msg)
	if err != nil {
		t.Fatalf("Failed to append message: %v", err)
	}

	// Should now be 1
	offset, err = storage.GetNextOffset(topic, partition)
	if err != nil {
		t.Fatalf("Failed to get next offset: %v", err)
	}
	if offset != 1 {
		t.Errorf("Expected offset 1 after append, got %d", offset)
	}
}

func TestPebbleStorage_GetPartitionSize(t *testing.T) {
	tmpDir := t.TempDir()
	
	storage, err := NewPebbleStorage(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create Pebble storage: %v", err)
	}
	defer storage.Close()

	topic := "test-topic"
	partition := int32(0)

	// Initially should be 0
	size, err := storage.GetPartitionSize(topic, partition)
	if err != nil {
		t.Fatalf("Failed to get partition size: %v", err)
	}
	if size != 0 {
		t.Errorf("Expected initial size 0, got %d", size)
	}

	// Add messages
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		msg := &Message{Key: "test", Value: []byte("test")}
		_, err = storage.Append(topic, partition, msg)
		if err != nil {
			t.Fatalf("Failed to append message: %v", err)
		}
	}

	// Should now be numMessages
	size, err = storage.GetPartitionSize(topic, partition)
	if err != nil {
		t.Fatalf("Failed to get partition size: %v", err)
	}
	if size != int64(numMessages) {
		t.Errorf("Expected size %d, got %d", numMessages, size)
	}
}

func TestPebbleStorage_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	topic := "test-topic"
	partition := int32(0)
	
	// Create storage and add messages
	{
		storage, err := NewPebbleStorage(tmpDir)
		if err != nil {
			t.Fatalf("Failed to create Pebble storage: %v", err)
		}

		for i := 0; i < 10; i++ {
			msg := &Message{
				Key:   "key-" + string(rune(i)),
				Value: []byte("value-" + string(rune(i))),
			}
			_, err = storage.Append(topic, partition, msg)
			if err != nil {
				t.Fatalf("Failed to append message: %v", err)
			}
		}

		storage.Close()
	}

	// Reopen storage and verify data persisted
	{
		storage, err := NewPebbleStorage(tmpDir)
		if err != nil {
			t.Fatalf("Failed to reopen Pebble storage: %v", err)
		}
		defer storage.Close()

		messages, hasMore, err := storage.Read(topic, partition, 0, 100)
		if err != nil {
			t.Fatalf("Failed to read messages after reopen: %v", err)
		}

		if len(messages) != 10 {
			t.Errorf("Expected 10 persisted messages, got %d", len(messages))
		}

		if hasMore {
			t.Error("Expected hasMore to be false")
		}

		// Verify next offset
		nextOffset, err := storage.GetNextOffset(topic, partition)
		if err != nil {
			t.Fatalf("Failed to get next offset: %v", err)
		}
		if nextOffset != 10 {
			t.Errorf("Expected next offset 10, got %d", nextOffset)
		}
	}
}