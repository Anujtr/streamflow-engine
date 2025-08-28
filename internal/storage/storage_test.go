package storage

import (
	"testing"
)

func TestNewStorage(t *testing.T) {
	storage := NewStorage()

	if storage == nil {
		t.Error("Expected non-nil storage")
	}

	if storage.topics == nil {
		t.Error("Expected non-nil topics map")
	}

	topics := storage.ListTopics()
	if len(topics) != 0 {
		t.Errorf("Expected 0 topics initially, got %d", len(topics))
	}
}

func TestStorage_CreateTopic(t *testing.T) {
	storage := NewStorage()

	err := storage.CreateTopic("test-topic", 4)
	if err != nil {
		t.Errorf("Unexpected error creating topic: %v", err)
	}

	// Try to create same topic again
	err = storage.CreateTopic("test-topic", 2)
	if err == nil {
		t.Error("Expected error when creating duplicate topic")
	}

	topics := storage.ListTopics()
	if len(topics) != 1 {
		t.Errorf("Expected 1 topic, got %d", len(topics))
	}

	if topics[0] != "test-topic" {
		t.Errorf("Expected topic name 'test-topic', got %s", topics[0])
	}
}

func TestStorage_GetTopic(t *testing.T) {
	storage := NewStorage()

	// Try to get non-existent topic
	_, err := storage.GetTopic("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent topic")
	}

	// Create and get topic
	storage.CreateTopic("test-topic", 2)
	topic, err := storage.GetTopic("test-topic")
	if err != nil {
		t.Errorf("Unexpected error getting topic: %v", err)
	}

	if topic.Name != "test-topic" {
		t.Errorf("Expected topic name 'test-topic', got %s", topic.Name)
	}

	if topic.NumPartitions != 2 {
		t.Errorf("Expected 2 partitions, got %d", topic.NumPartitions)
	}
}

func TestStorage_ProduceConsume(t *testing.T) {
	storage := NewStorage()

	// Try to produce to non-existent topic
	msg := &Message{
		Key:   "test-key",
		Value: []byte("test-value"),
	}

	_, _, err := storage.Produce("non-existent", msg)
	if err == nil {
		t.Error("Expected error producing to non-existent topic")
	}

	// Create topic and produce
	storage.CreateTopic("test-topic", 4)
	partition, offset, err := storage.Produce("test-topic", msg)
	if err != nil {
		t.Errorf("Unexpected error producing: %v", err)
	}

	if partition < 0 || partition >= 4 {
		t.Errorf("Invalid partition %d", partition)
	}

	if offset != 0 {
		t.Errorf("Expected first offset to be 0, got %d", offset)
	}

	// Consume the message
	messages, _, err := storage.Consume("test-topic", partition, 0, 1)
	if err != nil {
		t.Errorf("Unexpected error consuming: %v", err)
	}

	if len(messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(messages))
		return
	}

	// hasMore can be true or false depending on partition state

	consumedMsg := messages[0]
	if consumedMsg.Key != msg.Key {
		t.Errorf("Expected key %s, got %s", msg.Key, consumedMsg.Key)
	}

	if string(consumedMsg.Value) != string(msg.Value) {
		t.Errorf("Expected value %s, got %s", string(msg.Value), string(consumedMsg.Value))
	}
}

func TestStorage_GetTopicInfo(t *testing.T) {
	storage := NewStorage()

	// Try to get info for non-existent topic
	_, err := storage.GetTopicInfo("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent topic")
	}

	// Create topic and get info
	storage.CreateTopic("test-topic", 3)
	info, err := storage.GetTopicInfo("test-topic")
	if err != nil {
		t.Errorf("Unexpected error getting topic info: %v", err)
	}

	if len(info) != 3 {
		t.Errorf("Expected info for 3 partitions, got %d", len(info))
	}

	// All partitions should be empty initially
	for i := int32(0); i < 3; i++ {
		partitionInfo, exists := info[i]
		if !exists {
			t.Errorf("Expected info for partition %d", i)
		}
		if partitionInfo.Size != 0 {
			t.Errorf("Expected partition %d to be empty, got size %d", i, partitionInfo.Size)
		}
	}
}

func TestStorage_MultipleTopics(t *testing.T) {
	storage := NewStorage()

	// Create multiple topics
	topics := []string{"topic-1", "topic-2", "topic-3"}
	for _, topic := range topics {
		err := storage.CreateTopic(topic, 2)
		if err != nil {
			t.Errorf("Failed to create topic %s: %v", topic, err)
		}
	}

	// List topics
	listedTopics := storage.ListTopics()
	if len(listedTopics) != len(topics) {
		t.Errorf("Expected %d topics, got %d", len(topics), len(listedTopics))
	}

	// Verify each topic exists and is independent
	for _, topicName := range topics {
		topic, err := storage.GetTopic(topicName)
		if err != nil {
			t.Errorf("Failed to get topic %s: %v", topicName, err)
		}

		if topic.NumPartitions != 2 {
			t.Errorf("Expected topic %s to have 2 partitions, got %d", topicName, topic.NumPartitions)
		}

		// Produce a message to this topic
		msg := &Message{
			Key:   "key-" + topicName,
			Value: []byte("value-" + topicName),
		}

		_, _, err = storage.Produce(topicName, msg)
		if err != nil {
			t.Errorf("Failed to produce to topic %s: %v", topicName, err)
		}
	}

	// Verify topics are independent
	for _, topicName := range topics {
		info, err := storage.GetTopicInfo(topicName)
		if err != nil {
			t.Errorf("Failed to get info for topic %s: %v", topicName, err)
		}

		totalMessages := int64(0)
		for _, partitionInfo := range info {
			totalMessages += partitionInfo.Size
		}

		if totalMessages != 1 {
			t.Errorf("Expected topic %s to have 1 message, got %d", topicName, totalMessages)
		}
	}
}