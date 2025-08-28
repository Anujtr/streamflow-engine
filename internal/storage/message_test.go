package storage

import (
	"fmt"
	"testing"
	"time"

	pb "github.com/Anujtr/streamflow-engine/api/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMessage_ToProto(t *testing.T) {
	msg := &Message{
		Key:       "test-key",
		Value:     []byte("test-value"),
		Partition: 1,
		Offset:    42,
		Timestamp: time.Now(),
	}

	proto := msg.ToProto()

	if proto.Key != msg.Key {
		t.Errorf("Expected key %s, got %s", msg.Key, proto.Key)
	}
	if string(proto.Value) != string(msg.Value) {
		t.Errorf("Expected value %s, got %s", string(msg.Value), string(proto.Value))
	}
	if proto.Partition != msg.Partition {
		t.Errorf("Expected partition %d, got %d", msg.Partition, proto.Partition)
	}
	if proto.Offset != msg.Offset {
		t.Errorf("Expected offset %d, got %d", msg.Offset, proto.Offset)
	}
}

func TestFromProto(t *testing.T) {
	timestamp := time.Now()
	proto := &pb.Message{
		Key:       "test-key",
		Value:     []byte("test-value"),
		Partition: 1,
		Offset:    42,
		Timestamp: timestamppb.New(timestamp),
	}

	msg := FromProto(proto)

	if msg.Key != proto.Key {
		t.Errorf("Expected key %s, got %s", proto.Key, msg.Key)
	}
	if string(msg.Value) != string(proto.Value) {
		t.Errorf("Expected value %s, got %s", string(proto.Value), string(msg.Value))
	}
	if msg.Partition != proto.Partition {
		t.Errorf("Expected partition %d, got %d", proto.Partition, msg.Partition)
	}
	if msg.Offset != proto.Offset {
		t.Errorf("Expected offset %d, got %d", proto.Offset, msg.Offset)
	}
}

func TestPartition_Append(t *testing.T) {
	partition := NewPartition(0)
	
	msg := &Message{
		Key:   "test-key",
		Value: []byte("test-value"),
	}

	offset := partition.Append(msg)

	if offset != 0 {
		t.Errorf("Expected first offset to be 0, got %d", offset)
	}

	if partition.NextOffset != 1 {
		t.Errorf("Expected next offset to be 1, got %d", partition.NextOffset)
	}

	if len(partition.Messages) != 1 {
		t.Errorf("Expected 1 message in partition, got %d", len(partition.Messages))
	}

	storedMsg := partition.Messages[0]
	if storedMsg.Offset != 0 {
		t.Errorf("Expected stored message offset to be 0, got %d", storedMsg.Offset)
	}
	if storedMsg.Partition != 0 {
		t.Errorf("Expected stored message partition to be 0, got %d", storedMsg.Partition)
	}
}

func TestPartition_Read(t *testing.T) {
	partition := NewPartition(0)

	// Add some messages
	for i := 0; i < 5; i++ {
		msg := &Message{
			Key:   fmt.Sprintf("key-%d", i),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		partition.Append(msg)
	}

	// Read from beginning
	messages, hasMore := partition.Read(0, 3)
	
	if len(messages) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(messages))
	}
	
	if !hasMore {
		t.Error("Expected hasMore to be true")
	}

	// Check message order
	for i, msg := range messages {
		expectedKey := fmt.Sprintf("key-%d", i)
		if msg.Key != expectedKey {
			t.Errorf("Expected message %d key to be %s, got %s", i, expectedKey, msg.Key)
		}
	}

	// Read remaining messages
	messages, hasMore = partition.Read(3, 5)
	
	if len(messages) != 2 {
		t.Errorf("Expected 2 messages, got %d", len(messages))
	}
	
	if hasMore {
		t.Error("Expected hasMore to be false")
	}
}

func TestPartition_ReadInvalidOffset(t *testing.T) {
	partition := NewPartition(0)

	// Try to read from invalid offset
	messages, hasMore := partition.Read(10, 1)
	
	if messages != nil {
		t.Error("Expected nil messages for invalid offset")
	}
	
	if hasMore {
		t.Error("Expected hasMore to be false for invalid offset")
	}
}

func TestPartition_ConcurrentAccess(t *testing.T) {
	partition := NewPartition(0)
	
	// Start multiple goroutines writing
	go func() {
		for i := 0; i < 100; i++ {
			msg := &Message{
				Key:   fmt.Sprintf("key-1-%d", i),
				Value: []byte(fmt.Sprintf("value-1-%d", i)),
			}
			partition.Append(msg)
		}
	}()
	
	go func() {
		for i := 0; i < 100; i++ {
			msg := &Message{
				Key:   fmt.Sprintf("key-2-%d", i),
				Value: []byte(fmt.Sprintf("value-2-%d", i)),
			}
			partition.Append(msg)
		}
	}()
	
	// Give time for writes
	time.Sleep(100 * time.Millisecond)
	
	// Should have 200 messages total
	if partition.Size() != 200 {
		t.Errorf("Expected 200 messages after concurrent writes, got %d", partition.Size())
	}
	
	if partition.GetNextOffset() != 200 {
		t.Errorf("Expected next offset to be 200, got %d", partition.GetNextOffset())
	}
}