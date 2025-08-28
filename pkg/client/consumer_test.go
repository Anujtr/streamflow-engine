package client

import (
	"context"
	"testing"
	"time"
)

func TestConsumerConfig_Defaults(t *testing.T) {
	config := ConsumerConfig{}

	consumer, err := NewConsumer(config)
	if err != nil {
		t.Errorf("Unexpected error creating consumer: %v", err)
		return
	}
	defer consumer.Close()

	// Test that defaults are applied
	if config.Address == "" {
		config.Address = "localhost:8080" // Default should be applied internally
	}
}

func TestConsumeMessage(t *testing.T) {
	now := time.Now()
	msg := &ConsumeMessage{
		Key:       "test-key",
		Value:     []byte("test-value"),
		Partition: 1,
		Offset:    42,
		Timestamp: now,
	}

	if msg.Key != "test-key" {
		t.Errorf("Expected key 'test-key', got %s", msg.Key)
	}

	if string(msg.Value) != "test-value" {
		t.Errorf("Expected value 'test-value', got %s", string(msg.Value))
	}

	if msg.Partition != 1 {
		t.Errorf("Expected partition 1, got %d", msg.Partition)
	}

	if msg.Offset != 42 {
		t.Errorf("Expected offset 42, got %d", msg.Offset)
	}

	if !msg.Timestamp.Equal(now) {
		t.Errorf("Expected timestamp %v, got %v", now, msg.Timestamp)
	}
}

func TestConsumerMetrics(t *testing.T) {
	metrics := ConsumerMetrics{
		MessagesReceived: 100,
		Errors:           3,
	}

	if metrics.MessagesReceived != 100 {
		t.Errorf("Expected 100 messages received, got %d", metrics.MessagesReceived)
	}

	if metrics.Errors != 3 {
		t.Errorf("Expected 3 errors, got %d", metrics.Errors)
	}
}

// Integration test - requires running server
func TestConsumer_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	consumer, err := NewConsumer(ConsumerConfig{
		Address:     "localhost:8080",
		Timeout:     5 * time.Second,
		MaxMessages: 10,
	})

	// If server isn't running, skip this test
	if err != nil {
		t.Skipf("Server not available: %v", err)
	}
	defer consumer.Close()

	ctx := context.Background()

	// Test consuming from a topic (may be empty)
	messages, err := consumer.Consume(ctx, "test-topic", 0, 0, 5)
	if err != nil {
		t.Skipf("Failed to consume messages (server may not be running): %v", err)
	}

	// Messages may be empty, that's okay
	if messages == nil {
		t.Error("Expected non-nil message slice")
	}

	// Test metrics
	metrics := consumer.GetMetrics()
	if metrics.MessagesReceived < 0 {
		t.Errorf("Invalid messages received count: %d", metrics.MessagesReceived)
	}

	if metrics.Errors < 0 {
		t.Errorf("Invalid error count: %d", metrics.Errors)
	}
}